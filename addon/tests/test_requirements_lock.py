"""The lock files must agree with the ranges they were compiled from.

`requirements.txt` and `pyproject.toml` are what people edit: ranges, read by
pip, Dependabot and anyone installing from source. `requirements.lock` is what
the image installs, every transitive package pinned with hashes, and
`requirements-dev.lock` is what CI tests with. They are four files saying one
thing, so they can drift, and a drift is silent: the image keeps installing the
old pin while the range claims something else.

2.3.0 is why the image installs a lock at all. Built from ranges, it picked up
whatever amqtt PyPI had that day, 0.12.2, whose changed internals killed the
broker at start.

No network: this parses the files and compares versions, nothing more. When it
fails, regenerate the lock with the command in CONTRIBUTING.md
("Dependencies") rather than editing pins by hand -- a hand edit has no hashes.
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

ADDON = Path(__file__).resolve().parent.parent
REGENERATE = "regenerate the lock: see CONTRIBUTING.md, 'Dependencies'"

#: `name==version`, then an optional `; marker`, then the `\` continuation
#: that the `--hash` lines hang from. Every requirement line of a uv lock.
_PIN = re.compile(r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[^\s;\\]+)")


def _requirements(path: Path) -> list[Requirement]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [Requirement(line.split("#", 1)[0].strip()) for line in lines
            if line.split("#", 1)[0].strip()]


def _lock(path: Path) -> dict[str, tuple[Version, list[str]]]:
    """Canonical name -> (pinned version, its hashes), for one lock file."""
    pins: dict[str, tuple[Version, list[str]]] = {}
    current: list[str] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _PIN.match(line)
        if m:
            name = canonicalize_name(m["name"])
            assert name not in pins, f"{path.name}: {name} is pinned twice"
            current = []
            pins[name] = (Version(m["version"]), current)
        elif line.strip().startswith("--hash=") and current is not None:
            current.append(line.strip().removeprefix("--hash=").rstrip(" \\"))
        elif line.strip() and not line.startswith((" ", "#")):
            pytest.fail(f"{path.name}: not a pinned requirement: {line!r}")
    return pins


def _pyproject() -> dict:
    return tomllib.loads((ADDON / "pyproject.toml").read_text(encoding="utf-8"))["project"]


RUNTIME_LOCK = _lock(ADDON / "requirements.lock")
DEV_LOCK = _lock(ADDON / "requirements-dev.lock")


def _assert_satisfied(reqs: list[Requirement], lock: dict, lock_name: str) -> None:
    for req in reqs:
        name = canonicalize_name(req.name)
        assert name in lock, f"{req} is not in {lock_name}; {REGENERATE}"
        version = lock[name][0]
        assert req.specifier.contains(version, prereleases=True), (
            f"{lock_name} pins {name}=={version}, outside {req}; {REGENERATE}")


def test_every_runtime_requirement_is_pinned_within_its_range():
    _assert_satisfied(_requirements(ADDON / "requirements.txt"), RUNTIME_LOCK,
                      "requirements.lock")


def test_extras_brought_their_packages_into_the_lock():
    # A pin of `sqlalchemy` alone would satisfy the test above while the
    # `[asyncio]` extra's greenlet is missing -- and without greenlet every
    # await on the async engine raises. The lock is installed `--no-deps`, so
    # nothing else would pull it in.
    assert "greenlet" in RUNTIME_LOCK


def test_requirements_txt_and_pyproject_declare_the_same_ranges():
    # The image installs from requirements.txt's lock, a source install from
    # pyproject.toml: they are one list written twice.
    txt = {str(r) for r in _requirements(ADDON / "requirements.txt")}
    toml = {str(Requirement(d)) for d in _pyproject()["dependencies"]}
    assert txt == toml


def test_every_pin_is_exact_and_hashed():
    # pip's --require-hashes refuses a pin with no hash at build time; this
    # says so in the suite, on every platform, before an image is built.
    for name, (version, hashes) in {**RUNTIME_LOCK, **DEV_LOCK}.items():
        assert hashes, f"{name}=={version} has no --hash; {REGENERATE}"
        assert all(h.startswith("sha256:") for h in hashes), name


def test_ci_tests_exactly_what_the_image_ships():
    # CI installs requirements-dev.lock and nothing else, so every runtime pin
    # must be in it, at the same version, or the suite passes against a
    # package the image does not have.
    for name, (version, _) in RUNTIME_LOCK.items():
        assert name in DEV_LOCK, f"requirements-dev.lock lacks {name}; {REGENERATE}"
        assert DEV_LOCK[name][0] == version, (
            f"requirements-dev.lock pins {name}=={DEV_LOCK[name][0]}, the image "
            f"{version}; {REGENERATE}")


def test_dev_tools_are_pinned_within_their_ranges():
    dev = [Requirement(d) for d in _pyproject()["optional-dependencies"]["dev"]]
    _assert_satisfied(dev, DEV_LOCK, "requirements-dev.lock")


def test_the_image_installs_the_lock_and_only_the_lock():
    dockerfile = (ADDON / "Dockerfile").read_text(encoding="utf-8")
    installs = [line for line in dockerfile.splitlines() if "pip install" in line]
    assert installs, "the Dockerfile installs nothing with pip"
    for line in installs:
        assert "--require-hashes" in line and "--no-deps" in line, line
        assert "-r requirements.lock" in line, line
    assert "COPY requirements.lock" in dockerfile
