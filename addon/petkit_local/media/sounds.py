"""Custom feeding sounds in the one format the feeder can play.

A camera feeder downloads an uploaded sound and keeps it as
`/opt/user_feed_over_<id>.aac` (the name is the firmware's, `ctrl` strings:
`%s%s_%d.aac`, `rm /opt/user_feed_over*.aac`), then plays it through the same
ADTS parser as its own voice prompts. Those prompts are ADTS AAC-LC, 16 kHz,
mono, made with ffmpeg -- so that is the only format a download can be. An
.m4a from a phone or an .mp3 is downloaded without complaint and fails
silently at playback. Every upload is therefore transcoded here, and the
duration the device is handed in `dev_sound_get` is read from the result.

The format is the reading of dernassesmart's fork (YumShare Dual-Hopper 2,
firmware 895); nothing here has been played on a feeder by this repository.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
from typing import Any

from petkit_local.media.transcode import have_ffmpeg, run_ffmpeg
from petkit_local.utils.jsonio import atomic_write_bytes, atomic_write_json, read_bytes, read_json
from petkit_local.utils.paths import UnsafePathError, safe_join

log = logging.getLogger(__name__)

#: A transcode of a two-megabyte upload is seconds; this bounds a hung child.
SOUND_TIMEOUT = 120.0

#: What the device is handed, at most. The upload is capped before it gets
#: here, but two megabytes of a low-bitrate codec is most of an hour of audio,
#: and the transcode comes back through a pipe into memory.
MAX_ADTS_SIZE = 2 * 1024 * 1024

#: The demuxers an upload may be read with. An upload is a file somebody chose
#: in a browser, and ffmpeg picks the demuxer from its CONTENT: a text file
#: shaped like a concat script or a playlist is read as one, and names other
#: files for ffmpeg to open. ffmpeg's own guards stop the obvious spellings,
#: not all of them (a bare filename in a concat script is followed). Audio
#: containers only.
INPUT_FORMATS = "aac,mp3,wav,w64,mov,ogg,flac,matroska,amr,aiff,caf,asf"

ADTS_SAMPLE_RATES = (96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050,
                     16000, 12000, 11025, 8000, 7350)

#: Serialises every read-modify-write of a `sounds.json`. An upload and the
#: startup conversion both await ffmpeg between reading the list and writing
#: it back, and the second writer would otherwise drop the first one's entry.
META_LOCK = asyncio.Lock()


class TranscodeError(RuntimeError):
    """The upload could not be turned into ADTS AAC. The text is for the panel."""


def _frame_length(data: bytes, i: int) -> int:
    return ((data[i + 3] & 0x03) << 11) | (data[i + 4] << 3) | (data[i + 5] >> 5)


def _is_adts_header(data: bytes, i: int) -> bool:
    # 12 sync bits, then the two LAYER bits, which ADTS fixes at zero. Without
    # the layer test an MP3 passes: its sync word is the same eleven ones and
    # `FF FB` (MPEG-1 Layer III) has the twelfth set as well.
    return data[i] == 0xFF and (data[i + 1] & 0xF6) == 0xF0


def is_adts(data: bytes) -> bool:
    """Whether `data` starts with an ADTS frame whose length fits the buffer."""
    if len(data) < 7 or not _is_adts_header(data, 0):
        return False
    return 7 <= _frame_length(data, 0) <= len(data)


def is_playable(data: bytes) -> bool:
    """Whether `data` is already what a transcode would produce: ADTS, 16 kHz, mono."""
    if not is_adts(data):
        return False
    rate_index = (data[2] >> 2) & 0x0F
    channels = ((data[2] & 0x01) << 2) | (data[3] >> 6)
    return rate_index == ADTS_SAMPLE_RATES.index(16000) and channels == 1


def adts_duration_seconds(data: bytes) -> float:
    """Frames x 1024 samples / sample rate, walked frame by frame."""
    i, frames, rate = 0, 0, 0
    while i + 7 <= len(data):
        if not _is_adts_header(data, i):
            i += 1
            continue
        rate_index = (data[i + 2] >> 2) & 0x0F
        rate = ADTS_SAMPLE_RATES[rate_index] if rate_index < len(ADTS_SAMPLE_RATES) else 0
        length = _frame_length(data, i)
        if length < 7:
            break
        frames += 1
        i += length
    return frames * 1024 / rate if rate else 0.0


def _stage(data: bytes) -> str:
    fd, path = tempfile.mkstemp(prefix="petkit_sound_")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    return path


def _unlink(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


async def transcode_to_adts(data: bytes) -> bytes:
    """`data` (any audio container ffmpeg reads) as ADTS AAC-LC 16 kHz mono.

    The input goes through a temp file, not a pipe: an .m4a whose `moov` atom
    sits at the end (most phone recordings) cannot be parsed from a
    non-seekable stdin. It is staged in the system temp directory rather than
    beside the sounds, which is a directory two listeners serve from. The
    output is streamed back on stdout, ADTS being exactly the kind of format
    that can be.

    Raises:
        TranscodeError: ffmpeg is missing, timed out, could not decode the
            input, or the result is longer than a device is handed.
    """
    if not have_ffmpeg():
        raise TranscodeError("ffmpeg is not installed, so no sound can be converted")
    src = await asyncio.to_thread(_stage, data)
    try:
        rc, out, err = await run_ffmpeg(
            ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
             # Both are INPUT options: only local files, only audio containers.
             "-protocol_whitelist", "file", "-format_whitelist", INPUT_FORMATS,
             "-i", src, "-map", "0:a:0", "-vn",
             "-ar", "16000", "-ac", "1", "-c:a", "aac", "-b:a", "48k",
             # One frame past the cap, so "too long" is told apart from "fits".
             "-fs", str(MAX_ADTS_SIZE + 4096),
             "-f", "adts", "pipe:1"],
            timeout=SOUND_TIMEOUT, what="sound upload")
    finally:
        await asyncio.to_thread(_unlink, src)
    if rc == -2:
        raise TranscodeError("the conversion timed out")
    if rc != 0 or not is_adts(out):
        # ffmpeg's own last line names the temp file; the panel is told what
        # it means instead.
        log.info("ffmpeg refused a sound upload (exit %s): %s", rc,
                 err.decode("utf-8", "replace").strip()[-300:])
        raise TranscodeError("not an audio file ffmpeg can decode")
    if len(out) > MAX_ADTS_SIZE:
        raise TranscodeError(
            f"too long: the converted sound may be at most {MAX_ADTS_SIZE // 1024} KB")
    return out


def describe(adts: bytes) -> dict[str, Any]:
    """The entry fields that depend on the file: size, digest, duration."""
    return {
        "size": len(adts),
        "digest": hashlib.md5(adts).hexdigest(),
        "duration": int(round(adts_duration_seconds(adts))),
    }


async def migrate_sounds(data_dir: str) -> int:
    """Convert sounds stored as they were uploaded to ADTS, once.

    Walks `{data_dir}/sounds/<device>/sounds.json`; an entry whose file is not
    ADTS 16 kHz mono is transcoded under a `.aac` name and its size, digest
    and duration rewritten. The device then sees a new digest in
    `dev_sound_get` and downloads the playable file. A file that cannot be
    converted is left as it was and logged.

    Runs as a background task, so it never holds the add-on's start, and
    never raises anything but `CancelledError`.

    Returns:
        How many files were converted.
    """
    root = os.path.join(data_dir, "sounds")
    try:
        device_ids = await asyncio.to_thread(os.listdir, root)
    except OSError:
        return 0
    converted = 0
    for device_id in device_ids:
        try:
            converted += await _migrate_device(root, device_id)
        except Exception:  # noqa: BLE001 - one bad directory must not end the walk
            log.warning("Sound conversion skipped for device %s", device_id, exc_info=True)
    return converted


async def _migrate_device(root: str, device_id: str) -> int:
    try:
        d_dir = safe_join(root, device_id)
    except UnsafePathError:
        return 0
    meta = os.path.join(d_dir, "sounds.json")
    converted = 0
    async with META_LOCK:
        sounds = await asyncio.to_thread(read_json, meta, None)
        if not isinstance(sounds, list):
            return 0
        changed = False
        for entry in sounds:
            if not isinstance(entry, dict) or not isinstance(entry.get("id"), int):
                continue
            try:
                path = safe_join(d_dir, str(entry.get("filename", "")))
                data = await asyncio.to_thread(read_bytes, path)
            except (UnsafePathError, OSError):
                continue
            if is_playable(data):
                if not entry.get("duration"):
                    entry["duration"] = int(round(adts_duration_seconds(data)))
                    changed = True
                continue
            try:
                adts = await transcode_to_adts(data)
            except TranscodeError as e:
                log.warning("Sound %s of device %s stays unconverted (%s); upload it again",
                            entry.get("name"), device_id, e)
                continue
            new_name = f"sound_{entry['id']}.aac"
            new_path = os.path.join(d_dir, new_name)
            await asyncio.to_thread(atomic_write_bytes, new_path, adts)
            entry["filename"] = new_name
            entry.update(describe(adts))
            # The list is written before the old file goes, so a kill between
            # the two leaves a stray file rather than an entry naming nothing.
            await asyncio.to_thread(atomic_write_json, meta, sounds)
            changed = False
            if os.path.realpath(new_path) != path:
                await asyncio.to_thread(_unlink, path)
            converted += 1
            log.info("Converted sound %s of device %s to ADTS AAC (%d s)",
                     entry.get("name"), device_id, entry["duration"])
        if changed:
            await asyncio.to_thread(atomic_write_json, meta, sounds)
    return converted
