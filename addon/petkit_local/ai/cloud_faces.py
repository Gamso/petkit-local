"""The cloud's face photos, normalised before a device is handed them.

In proxy mode `dev_discern_pic` is PetKit's answer: the account's pets and the
URLs of their reference photos. The device fetches each one into
`/tmp/pet_face_pic.jpg` and its camera process decodes it. Those photos are
normally 224x224 face crops, but nothing guarantees it, and one that is not
kills a small camera: a D4SH (92 MB RAM) handed a raw 4096x3072 photo ran out of
memory in OpenCV on every start of `media`, restarted it every ~82 s and never
got further (issue #129). And `wget -c` keeps that file in `/tmp` until reboot.

So redaction rewrites every `discern[].url` to OUR `/faces/` route
(`http/redact/rules.py::_match_faces`), under a name this module chooses
(`cloud_face_filename`), and this module answers it with the cloud's photo put
through `normalize_face_photo` — the same 224x224 every local upload gets. A
photo that cannot be fetched, is not a JPEG, or does not come out of the
normaliser at exactly that size is never served at all.

**This is not a poller.** A photo is fetched only because a device asked: the
proxied `dev_discern_pic` it is named in (`prefetch`, bounded, while that reply
waits), or the device's own GET of the rewritten URL (`ensure`). Both are the
request the device would have sent PetKit's storage itself.

On disk, under `{data_dir}/faces_cloud/`:

* `cloud_<face id>.jpg` — the normalised photo, the only thing ever served;
* `index.json` — `{filename: original URL}`, so a URL a device already holds
  still resolves after a restart that lost the cached file. It holds only
  what the cloud's reply named; the photo store's addresses are no secret
  from the device, which was handed them verbatim before this existed.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from petkit_local.ai.pets import _NON_URL_SAFE
from petkit_local.media.transcode import (
    FACE_PHOTO_SIZE, jpeg_dimensions, normalize_face_photo,
)
from petkit_local.utils.coerce import to_int
from petkit_local.utils.jsonio import atomic_write_bytes, atomic_write_json, read_json
from petkit_local.utils.paths import UnsafePathError, safe_join

log = logging.getLogger(__name__)

#: The cache's directory under `data_dir`. A sibling of `faces/`, never inside
#: it: `faces/` holds the owner's own photos and is listed by the panel.
CLOUD_FACES_DIR = "faces_cloud"
INDEX_FILE = "index.json"

#: Every cloud face's filename starts with this, which is how `/faces/` tells
#: one from a local `pet_<id>_face_<id>.jpg`.
FILENAME_PREFIX = "cloud_"

#: The whole shape a cloud face filename can take. It is built only by
#: `cloud_face_filename`, and the route refuses anything else under the prefix.
FILENAME_RE = re.compile(r"^cloud_[0-9a-z]{1,64}\.jpg$")

#: How long a proxied `dev_discern_pic` reply may wait for its photos, all of
#: them together (they are fetched concurrently). The device is waiting for
#: that reply, so this is short; a photo still in flight when it runs out keeps
#: its rewritten URL and is finished in the background, for the device's GET.
PREFETCH_TIMEOUT = 3.0

#: How long the device's own GET of a photo not yet cached may wait. Under the
#: firmware's `wget -T 10`, so the device gets an answer rather than a timeout;
#: the fetch carries on regardless and the next GET finds it cached.
SERVE_TIMEOUT = 8.0

#: One fetch of one photo, end to end. Independent of either wait above.
FETCH_TIMEOUT = aiohttp.ClientTimeout(total=20)

#: Larger than any photo worth normalising, small enough that a reply naming
#: something enormous cannot fill memory before it is refused. The photo in
#: issue #129 was 214 KB; a raw phone photo is a few MB.
MAX_DOWNLOAD_BYTES = 12 * 1024 * 1024


def cloud_face_filename(face_id: Any, url: str) -> str:
    """The `/faces/` filename a cloud face photo is served under.

    `cloud_<face id>.jpg` for the positive integer id the cloud sends (the
    firmware reads it with `%lld`), so the name a device is handed stays the
    same from one poll to the next. Anything else — a missing id, a string,
    `"1;rm -rf"` — is named by a hash of the URL instead, never by the id.

    The name ends up inside a SHELL COMMAND on the device
    (`wget -O /tmp/pet_face_pic.jpg -c <url>`, see
    `ai/pets.py::_safe_photo_filename`), so it is built from digits and hex only
    and then passed through the same whitelist as a local face's.
    """
    number = None if isinstance(face_id, bool) else to_int(face_id, None)
    if number is not None and 0 < number < 10 ** 20:
        stem = str(number)
    else:
        stem = "u" + hashlib.sha256(str(url).encode()).hexdigest()[:20]
    return f"{FILENAME_PREFIX}{_NON_URL_SAFE.sub('', stem)}.jpg"


def is_cloud_face(filename: str) -> bool:
    """Whether a `/faces/` filename belongs to this cache rather than `faces/`."""
    return filename.startswith(FILENAME_PREFIX)


def _same_photo(a: str, b: str) -> bool:
    """Whether two URLs name the same stored photo, ignoring the query string.

    A signed URL changes its query on every reply; treating that as a new photo
    would throw the cached one away and fetch it again at every poll.
    """
    pa, pb = urlsplit(a), urlsplit(b)
    return (pa.scheme, pa.netloc, pa.path) == (pb.scheme, pb.netloc, pb.path)


class CloudFaces:
    """The on-disk cache of normalised cloud face photos, and how it fills.

    Created per device-facing app (`http/server.py::create_app`) and touches
    the disk only when proxy mode first rewrites a reply: the directory and the
    index are created lazily, so a server that never proxies leaves no trace.
    """

    def __init__(self, root: str) -> None:
        self._root = root
        self._index: dict[str, str] | None = None
        #: One fetch per filename at a time, shared by every waiter. Holding
        #: the task here is also what keeps a fetch alive after the reply that
        #: started it has stopped waiting.
        self._inflight: dict[str, asyncio.Task[str | None]] = {}

    @property
    def root(self) -> str:
        """The cache directory."""
        return self._root

    # --- the index --------------------------------------------------------

    async def _load(self) -> dict[str, str]:
        if self._index is None:
            raw = await asyncio.to_thread(
                read_json, os.path.join(self._root, INDEX_FILE), {})
            self._index = {
                name: url for name, url in (raw.items() if isinstance(raw, dict) else ())
                if isinstance(name, str) and FILENAME_RE.match(name) and isinstance(url, str)
            }
        return self._index

    async def remember(self, faces: dict[str, str]) -> None:
        """Record which cloud URL each filename stands for, and persist it.

        A filename that now names a different photo (the owner replaced it in
        PetKit's app) loses its cached file, so the next request fetches the
        new one instead of serving the old.
        """
        index = await self._load()
        changed = False
        for name, url in faces.items():
            if not FILENAME_RE.match(name):
                continue
            old = index.get(name)
            if old == url:
                continue
            if old is not None and not _same_photo(old, url):
                await asyncio.to_thread(self._discard, name)
            index[name] = url
            changed = True
        if changed:
            try:
                await asyncio.to_thread(
                    atomic_write_json, os.path.join(self._root, INDEX_FILE), index)
            except OSError:
                # The in-memory index still serves this run; only a restart
                # before the photo is cached would lose the URL.
                log.warning("Could not persist the cloud face index in %s",
                            self._root, exc_info=True)

    # --- the photos -------------------------------------------------------

    def _path(self, filename: str) -> str | None:
        if not FILENAME_RE.match(filename):
            return None
        try:
            return safe_join(self._root, filename)
        except UnsafePathError:
            return None

    def _discard(self, filename: str) -> None:
        path = self._path(filename)
        if path is not None:
            try:
                os.remove(path)
            except OSError:
                pass

    def cached(self, filename: str) -> str | None:
        """The path of a photo already normalised and stored, or None."""
        path = self._path(filename)
        return path if path is not None and os.path.isfile(path) else None

    async def prefetch(self, faces: dict[str, str], session: aiohttp.ClientSession, *,
                       timeout: float | None = None) -> set[str]:
        """Fetch and normalise the photos one reply names, for at most `timeout`
        (`PREFETCH_TIMEOUT` by default).

        Returns:
            The filenames that definitely FAILED: not fetched, not a JPEG, or
            not normalisable to 224x224. The caller drops them from the reply.
            A photo still in flight at the deadline is not among them: it keeps
            its URL, and the fetch finishes in the background.
        """
        await self._load()
        tasks = {name: self._start(name, session) for name in faces if not self.cached(name)}
        tasks = {name: task for name, task in tasks.items() if task is not None}
        if not tasks:
            return set()
        done, _pending = await asyncio.wait(
            set(tasks.values()), timeout=PREFETCH_TIMEOUT if timeout is None else timeout)
        return {name for name, task in tasks.items()
                if task in done and not task.cancelled() and task.result() is None}

    async def ensure(self, filename: str, session: aiohttp.ClientSession | None, *,
                     timeout: float | None = None) -> str | None:
        """The cached photo's path, fetching it first if needed and allowed,
        waiting at most `timeout` (`SERVE_TIMEOUT` by default).

        `session` None means "serve only what is cached" — what `/faces/` asks
        when proxy mode is off, so a device holding an old cloud URL never
        makes this add-on reach PetKit once the owner has turned proxying off.
        """
        path = self.cached(filename)
        if path is not None or session is None:
            return path
        if filename not in await self._load():
            return None
        task = self._start(filename, session)
        if task is None:
            return None
        done, _pending = await asyncio.wait(
            {task}, timeout=SERVE_TIMEOUT if timeout is None else timeout)
        if task not in done:
            log.info("Cloud face %s is still being fetched; the device will ask again",
                     filename)
            return None
        return None if task.cancelled() else task.result()

    async def close(self, _app: object = None) -> None:
        """Cancel the fetches still running. An `on_cleanup` callback."""
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _start(self, filename: str, session: aiohttp.ClientSession) -> asyncio.Task | None:
        """The fetch for `filename`, joined if one is already running."""
        task = self._inflight.get(filename)
        if task is not None:
            return task
        url = (self._index or {}).get(filename)
        if url is None:
            return None
        task = asyncio.ensure_future(self._produce(filename, url, session))
        self._inflight[filename] = task
        task.add_done_callback(lambda _t, name=filename: self._inflight.pop(name, None))
        return task

    async def _produce(self, filename: str, url: str,
                       session: aiohttp.ClientSession) -> str | None:
        """Fetch, normalise and store one photo. None on any failure, logged."""
        path = self._path(filename)
        if path is None:
            return None
        try:
            data = await _download(session, url)
            if data is None:
                return None
            if data[:3] != b"\xff\xd8\xff":
                log.warning("Cloud face %s is not a JPEG — not handed to the device", url)
                return None
            photo = await normalize_face_photo(data)
            # The one guarantee this module exists for. `normalize_face_photo`
            # hands back its input when ffmpeg is missing or fails, which is
            # right for an upload and exactly wrong here.
            dims = jpeg_dimensions(photo)
            if dims != (FACE_PHOTO_SIZE, FACE_PHOTO_SIZE):
                log.warning("Cloud face %s could not be normalised (%s) — not handed "
                            "to the device", url,
                            f"{dims[0]}x{dims[1]}" if dims else "unreadable")
                return None
            current = (self._index or {}).get(filename)
            if current is None or not _same_photo(current, url):
                return None      # replaced while it was being fetched
            await asyncio.to_thread(atomic_write_bytes, path, photo)
            log.info("Cloud face %s cached as %s (%d bytes, was %s)", url, filename,
                     len(photo), "x".join(map(str, jpeg_dimensions(data) or ("?",))))
            return path
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("Could not cache the cloud face %s", url, exc_info=True)
            return None


async def _download(session: aiohttp.ClientSession, url: str) -> bytes | None:
    """One photo's bytes, or None. Read against a cap, not trusted to a header."""
    if urlsplit(url).scheme not in ("http", "https"):
        return None
    try:
        async with session.get(url, timeout=FETCH_TIMEOUT) as resp:
            if resp.status != 200:
                log.warning("Cloud face %s answered %d — not handed to the device",
                            url, resp.status)
                return None
            data = bytearray()
            async for chunk in resp.content.iter_chunked(64 * 1024):
                data.extend(chunk)
                if len(data) > MAX_DOWNLOAD_BYTES:
                    log.warning("Cloud face %s is over %d bytes — refused",
                                url, MAX_DOWNLOAD_BYTES)
                    return None
            return bytes(data)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("Could not fetch the cloud face %s: %s", url, e)
        return None


def drop_faces(body: bytes, failed: set[str]) -> tuple[bytes, list[tuple[str, str]]]:
    """Remove the faces whose photo failed from a rewritten reply.

    A `discern` entry goes when its URL ends in a failed filename, and a pet
    entry whose `discern` list that empties goes too — `discern_pic_payload`
    never lists a pet without photos either. Everything else is kept as it was,
    order included.

    Returns:
        The new body, and `(path, url)` for every face removed. The body is
        returned unchanged when it cannot be read or nothing matched.
    """
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return body, []
    removed: list[tuple[str, str]] = []

    def failed_url(face: Any) -> bool:
        if not isinstance(face, dict) or not isinstance(face.get("url"), str):
            return False
        return face["url"].rsplit("/", 1)[-1] in failed

    def walk(node: Any, path: str) -> Any:
        if isinstance(node, list):
            out = []
            for i, item in enumerate(node):
                got = walk(item, f"{path}[{i}]")
                if isinstance(item, dict) and isinstance(item.get("discern"), list) \
                        and item["discern"] and not got.get("discern"):
                    continue       # a pet left with no photo at all
                out.append(got)
            return out
        if isinstance(node, dict):
            out = {}
            for key, value in node.items():
                sub = f"{path}.{key}" if path else str(key)
                if key == "discern" and isinstance(value, list):
                    kept = []
                    for i, face in enumerate(value):
                        if failed_url(face):
                            removed.append((f"{sub}[{i}]", face["url"]))
                        else:
                            kept.append(face)
                    out[key] = kept
                else:
                    out[key] = walk(value, sub)
            return out
        return node

    cleaned = walk(data, "")
    if not removed:
        return body, []
    return json.dumps(cleaned).encode(), removed
