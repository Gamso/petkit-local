"""Is this a usable reference face photo? Checked on upload, before it is normalised.

The device's NPU matches visits against these photos, and a bad one costs
twice: recognition is worse, and an outsized one once took a D4SH's camera
pipeline down (#129). So one function, `assess_face_photo`, looks at the bytes
as uploaded — the panel's and the app API's uploads both — and says:

* a REFUSAL (`FACE_TOO_SMALL`) for what cannot work: under `MIN_FACE_SIDE` on
  the short side, the photo would have to be UPSCALED to the 224x224 the
  recogniser is fed, which invents the detail it matches on;
* WARNINGS, which never block: `blurry`, `too_dark`, `overexposed`,
  `not_square`. Stable codes, translated by whoever shows them.

No image library. The size and the aspect ratio come from the JPEG header
(`transcode.jpeg_dimensions`), which needs nothing. Blur and exposure need
pixels, and those come from ffmpeg, as the resize itself does: the same
centre crop and scale `normalize_face_photo` applies, as raw 8-bit grey — so
what is judged is exactly what the device will be handed. With no ffmpeg, or
an image ffmpeg cannot decode, those checks are reported as not done
(`checked: false`) and the upload goes ahead: never fail on a missing tool.

`python -m petkit_local.ai.face_quality <jpeg>...` prints the numbers behind
the verdicts, which is how the thresholds below are meant to be recalibrated.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from petkit_local.media import transcode
from petkit_local.media.transcode import FACE_PHOTO_SIZE, jpeg_dimensions

log = logging.getLogger(__name__)

#: The refusal: a photo smaller than this on its short side.
FACE_TOO_SMALL = "too_small"
#: The input size of the recogniser (`FACE_PHOTO_SIZE`). Anything smaller is
#: upscaled to it, so it is the floor rather than a preference.
MIN_FACE_SIDE = FACE_PHOTO_SIZE

#: The warnings. Stable codes: the phone app translates them.
WARN_BLURRY = "blurry"
WARN_TOO_DARK = "too_dark"
WARN_OVEREXPOSED = "overexposed"
WARN_NOT_SQUARE = "not_square"

#: Long side over short side above which a photo is probably not cropped to the
#: head. The crop to square then keeps only the middle, which may not be it.
MAX_ASPECT = 1.6

#: The grey image the pixel checks run on: the 224x224 centre crop the device
#: receives, not a thumbnail of the original. Blur only matters at the size the
#: recogniser sees it — a phone photo soft at 4000 px is sharp at 224.
ANALYSIS_SIZE = FACE_PHOTO_SIZE

#: Variance of the 4-neighbour Laplacian below which the crop is `blurry`.
#: PROVISIONAL: not yet calibrated on real reference photos. Chosen on a
#: synthetic 224x224 texture with a natural (1/f^0.5) spectrum and a 20..220
#: range: in focus it measures ~360, after a Gaussian blur of sigma 0.5 px
#: ~110, of sigma 1 px ~10, of 2 px ~3. 40 sits between "half a pixel soft",
#: which recognition tolerates, and "a pixel blurred", where fur texture is
#: gone. A flat, featureless photo (a wall, a lens cap) scores near 0 and is
#: flagged too, which is the right answer for it. Recalibrate with this
#: module's `__main__` over photos known good and bad.
BLUR_MIN_VARIANCE = 40.0

#: A grey level at or below this counts as crushed black, at or above
#: `BRIGHT_CLIP` as blown white.
DARK_CLIP = 16
BRIGHT_CLIP = 240
#: `too_dark`: a mean below `DARK_MEAN`, or more than `CLIPPED_SHARE` of the
#: pixels crushed. Set low on purpose, because a black cat well exposed is
#: still a dark picture; what is flagged is a face there is nothing left of.
DARK_MEAN = 40.0
#: `overexposed`: a mean above `BRIGHT_MEAN`, or more than `CLIPPED_SHARE`
#: blown. The mirror image, for a white cat in the sun.
BRIGHT_MEAN = 215.0
CLIPPED_SHARE = 0.35

#: One decode is one frame of a still: generous, as `THUMB_TIMEOUT` is.
ANALYSIS_TIMEOUT = transcode.THUMB_TIMEOUT


@dataclass
class FaceQuality:
    """What `assess_face_photo` found.

    `checked` is True only when the pixel checks ran (blur, exposure); the
    header checks (size, aspect) need no decoder and are applied whenever the
    header can be read, so `not_square` can come back with `checked` False.
    """

    checked: bool = False
    warnings: list[str] = field(default_factory=list)
    refusal: str | None = None
    width: int | None = None
    height: int | None = None
    #: The numbers behind the verdicts, for the log and for calibration.
    metrics: dict[str, float] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        """The `quality` object both upload responses carry."""
        return {"checked": self.checked, "warnings": list(self.warnings)}


def size_refusal(data: bytes) -> str | None:
    """`FACE_TOO_SMALL` if the header says the short side is under `MIN_FACE_SIDE`.

    A header that cannot be read refuses nothing: the size is unknown, not small.
    """
    dims = jpeg_dimensions(data)
    if dims and min(dims) < MIN_FACE_SIDE:
        return FACE_TOO_SMALL
    return None


def grey_metrics(pixels: bytes, width: int, height: int) -> dict[str, float]:
    """Mean, clipped shares and Laplacian variance of an 8-bit grey image.

    Pure Python and a few tens of milliseconds at 224x224. The Laplacian is the
    4-neighbour kernel over the interior (the border has no neighbours).
    """
    n = width * height
    hist = Counter(pixels[:n])
    mean = sum(level * count for level, count in hist.items()) / n
    dark = sum(c for level, c in hist.items() if level <= DARK_CLIP) / n
    bright = sum(c for level, c in hist.items() if level >= BRIGHT_CLIP) / n

    total = total_sq = 0
    count = 0
    for y in range(1, height - 1):
        row = y * width
        centre = pixels[row + 1:row + width - 1]
        left = pixels[row:row + width - 2]
        right = pixels[row + 2:row + width]
        up = pixels[row - width + 1:row - 1]
        down = pixels[row + width + 1:row + 2 * width - 1]
        for c, lt, rt, u, d in zip(centre, left, right, up, down, strict=True):
            v = lt + rt + u + d - 4 * c
            total += v
            total_sq += v * v
        count += len(centre)
    laplacian = (total_sq / count - (total / count) ** 2) if count else 0.0
    return {"mean": mean, "dark_share": dark, "bright_share": bright,
            "laplacian_var": laplacian}


def pixel_warnings(metrics: dict[str, float]) -> list[str]:
    """The warnings `grey_metrics`'s numbers call for, in a fixed order."""
    warnings = []
    if metrics["laplacian_var"] < BLUR_MIN_VARIANCE:
        warnings.append(WARN_BLURRY)
    if metrics["mean"] < DARK_MEAN or metrics["dark_share"] > CLIPPED_SHARE:
        warnings.append(WARN_TOO_DARK)
    if metrics["mean"] > BRIGHT_MEAN or metrics["bright_share"] > CLIPPED_SHARE:
        warnings.append(WARN_OVEREXPOSED)
    return warnings


async def _grey_crop(data: bytes, size: int = ANALYSIS_SIZE) -> bytes | None:
    """The centre crop `normalize_face_photo` keeps, as `size`x`size` grey bytes.

    None without ffmpeg or when it cannot decode the image — never raises.
    """
    if not transcode.have_ffmpeg():
        return None
    rc, stdout, stderr = await transcode.run_ffmpeg(
        ["ffmpeg", "-v", "error", "-i", "pipe:0",
         "-vf", f"crop='min(iw,ih)':'min(iw,ih)',scale={size}:{size}:flags=lanczos,format=gray",
         "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1"],
        timeout=ANALYSIS_TIMEOUT, what="face photo quality check", stdin_data=data)
    if rc != 0 or len(stdout) < size * size:
        log.info("face photo quality not checked, ffmpeg could not decode it: %s",
                 stderr.decode(errors="replace")[-300:])
        return None
    return stdout[:size * size]


async def assess_face_photo(data: bytes) -> FaceQuality:
    """Judge one uploaded photo, as uploaded (before `normalize_face_photo`).

    Never raises and never blocks an upload by itself: only `refusal` does, and
    the caller decides to honour it (`PetRegistry.face_refusal` does).
    """
    quality = FaceQuality()
    dims = jpeg_dimensions(data)
    if dims:
        quality.width, quality.height = dims
        quality.refusal = size_refusal(data)
        if quality.refusal:
            # Not decoded: the upload is refused whatever the pixels say.
            return quality
        if min(dims) and max(dims) / min(dims) > MAX_ASPECT:
            quality.warnings.append(WARN_NOT_SQUARE)

    pixels = await _grey_crop(data)
    if pixels is None:
        return quality
    quality.metrics = await asyncio.to_thread(grey_metrics, pixels, ANALYSIS_SIZE, ANALYSIS_SIZE)
    quality.warnings[:0] = pixel_warnings(quality.metrics)
    quality.checked = True
    if quality.warnings:
        log.info("face photo %s: %s (%s)",
                 f"{quality.width}x{quality.height}" if dims else "of unknown size",
                 ", ".join(quality.warnings),
                 ", ".join(f"{k}={v:.2f}" for k, v in quality.metrics.items()))
    return quality


def main(argv: list[str]) -> int:
    """Print each photo's numbers and verdict — the calibration aid."""
    if not argv:
        print("usage: python -m petkit_local.ai.face_quality photo.jpg [...]")
        return 2
    for path in argv:
        with open(path, "rb") as f:
            q = asyncio.run(assess_face_photo(f.read()))
        numbers = " ".join(f"{k}={v:.2f}" for k, v in q.metrics.items()) or "not checked"
        print(f"{path}: {q.width}x{q.height} {numbers} -> "
              f"{q.refusal or ', '.join(q.warnings) or 'ok'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
