# Contributing

## What helps

**A report that your model works.** This one is easy to skip, because nothing is
wrong. But one person owns one litter box, so every other model in the README is
inference until somebody says otherwise, and only an owner can change that. *"T4, everything
works"* is enough to move a model up the table, and issues that report nothing
wrong are welcome here. There is a
[Device report](https://github.com/alex-so-3/petkit-local/issues/new?template=device_report.yml)
template that asks for nothing you would have to go and collect.

Also welcome:

- **Payload captures**, when a model does something unexplained. Most remaining
  unknowns are "what does model X actually send?".
- **New models.** A codename in `utils/const.py`, plus any spelling differences in
  `devices/state_parsers.py`, is usually the whole change.
- **Entity fixes.** A wrong unit, a wrong device class, a control that does not
  take effect. Common on models nobody has tested, because their definitions come
  from a client of PetKit's *cloud* API and the cloud's field names are not always
  the device's.

## Before you attach a capture, read it

A capture is a verbatim recording of everything the device said and was told.
Nothing in it is filtered, because it is only useful if it is exact.

Any of these files can contain your **Wi-Fi SSID and BSSID**, your LAN addresses,
the device serial and its signing secret. Secrets are not confined to the files
that sound like they would hold them:

* a Purobot Ultra puts `packageSn` **and `packageSecret`** in every single
  `property/post`, roughly once a minute, and in its `melt_over` and
  `package_over` events;
* every camera event carries the `aesKey` its media is encrypted with, in clear;
* the Wi-Fi block rides along in the state snapshot attached to **every** event,
  not only in the state reports.

If proxy mode was on, `proxy_http.jsonl` and `proxy_mqtt.jsonl` also carry the
**full exchanges with PetKit, including your account credentials**, which is
enough for someone else to talk to their cloud as you.

`requests.jsonl`, `state_report.jsonl`, `mqtt.jsonl` and `event_report.jsonl`
usually answer the question on their own. Grep your SSID out first and attach only
what the question needs. Note that the redaction proxy mode applies to what
reaches the DEVICE does not apply to what is written here — a capture is
deliberately unfiltered.

## Working on the code

```sh
cd addon
pip install --require-hashes --no-deps -r requirements-dev.lock   # exactly what CI uses
pytest                              # the whole suite
pytest -k stitch                    # by name
pytest --firmware                   # also the patcher tests, if you have images
ruff check petkit_local/ tests/
```

`--firmware` runs the patcher integration tests against real device images,
which are multi-megabyte and not in the repo. Populate `addon/tests/firmware/`
first; the test module says what it expects. Without the flag those tests skip.

The suite needs no device, no broker and no network. CI runs it on Python 3.14, the
version the image ships, with the exact packages the image ships (`requirements-dev.lock`,
below), and imports every module so a runtime-only one cannot break unnoticed. A pull
request that changes `addon/Dockerfile`, `addon/requirements.txt`, `addon/requirements.lock`
or `addon/pyproject.toml` also builds the container image for amd64, arm64 and armv7
(`build-image.yml`), because a passing test suite says nothing about whether the
image builds; a change to the code alone cannot break that build, so it is not
paid for. A `v*` tag builds all three again before anything is published.

The panel's JavaScript and CSS are prettier-formatted, and CI checks it:

```sh
npx prettier@3 --write addon/petkit_local/web/static/js/*.js addon/petkit_local/web/static/styles.css
```

Please run the tests before opening a PR.

### The smoke test

The suite tests the code; `addon/tests/smoke/` tests the image. Every pull request
and push to `main` builds the amd64 image and runs it, and a `v*` tag publishes
nothing until it passes. 2.3.0 passed the whole suite and then died on every
start; this fails on that image in seconds. To run it yourself, with only Docker:

```sh
docker build -t petkit-local:smoke addon
bash addon/tests/smoke/run.sh petkit-local:smoke
```

`run.sh` starts the image with `docker-compose.yml`'s command (`--no-ha`), runs
`smoke.py` beside it in the same network namespace, and fails on a restart, a
traceback in the log, or any step `smoke.py` lists in its docstring: the device API,
the panel, the MQTT TLS listener with a device's cipher offer, and a fake T5 that
signs up, connects without subscribing, receives a panel command and posts telemetry.
On Windows, run it from a checkout with LF line endings (WSL, or a
`git -c core.autocrlf=false` clone); bash refuses a CRLF script.

### Dependencies

Two kinds of file, and only one is edited by hand:

| File | What it is | Edited |
|---|---|---|
| `addon/requirements.txt`, `pyproject.toml` `dependencies` | The ranges the add-on supports. One list written twice. | By hand, both together |
| `pyproject.toml` `[dev]` | The ranges of the test and lint tools. | By hand |
| `addon/requirements.lock` | Every package the image installs, transitive ones included, pinned with the hash of each wheel and sdist. | Generated |
| `addon/requirements-dev.lock` | The same pins plus the tools. What CI installs. | Generated |

The image installs `requirements.lock` with `pip install --require-hashes --no-deps`,
so nothing outside it can get in. Built from the ranges, 2.3.0 picked up whatever amqtt
PyPI had that day, and its broker died on every start. `tests/test_requirements_lock.py`
fails when the four files disagree, and its message points here.

The locks are made by [uv](https://docs.astral.sh/uv/) for every platform at once
(`--universal`), which is what lets one file serve Alpine amd64 and arm64 and Debian
armv7, the last compiling some packages from the sdist whose hash is in the lock. Run
it in a container, so the result does not depend on what is installed on your machine:

```sh
cd addon
docker run --rm -v "$PWD:/work" -w /work python:3.14-slim sh -c '
  pip install -q uv &&
  uv pip compile requirements.txt --universal --generate-hashes --python-version 3.14 \
      -o requirements.lock "$@" &&
  uv pip compile pyproject.toml --extra dev --universal --generate-hashes --python-version 3.14 \
      -c requirements.lock -o requirements-dev.lock "$@"' -- --upgrade-package amqtt
```

The trailing arguments say what to move, and are passed to both commands:

- **Nothing** (drop `-- --upgrade-package amqtt`): re-lock after editing a range.
  uv keeps every existing pin that still satisfies the ranges and changes only what
  it has to.
- **`--upgrade-package NAME`**, repeatable: move that package, transitive ones
  included, to the newest version the ranges allow, and its dependencies only as far
  as it needs.
- **`--upgrade`**: move everything. Worth doing on purpose, now and then, in a
  pull request of its own, since nothing else ever moves a transitive package.

Commit both locks with the range change that caused them; CI then builds the image
for all three architectures and smoke-tests it.

**What Dependabot does and does not do** (`.github/dependabot.yml`). It bumps the
base images' digests, the SHA-pinned actions, and the ranges of DIRECT dependencies
in `requirements.txt` and `pyproject.toml`. It cannot regenerate the locks: it does
not read `.lock` files, and its pip-compile support runs pip-compile, not uv. So a
Dependabot dependency PR fails `test_requirements_lock.py` until the command above
is run on its branch (`--upgrade-package` the package it bumped) and the locks are
pushed there. Transitive packages get no Dependabot PR at all.

## Things that are easy to break

- **Entity keys are user state.** Renaming one orphans that entity in every
  existing installation and loses its history. Labels can change freely; keys
  change only deliberately.
- **Protocol facts live in `events/codes.py`**, graded by the evidence behind
  them. Add a code there rather than in a private set somewhere, and say what
  convinced you. Where a capture and the firmware disagree, the row is marked
  `conflicted` rather than quietly picking one.
- **Nothing is written to a device on a guess.** A setting whose value has never
  been observed is left unset rather than given an invented default, because
  `dev_device_info` serves those values straight back to the device.

[`ARCHITECTURE.md`](ARCHITECTURE.md) is the map: what each package owns and how
a device request travels through them. The invariants worth knowing before a
larger change are collected in [`AGENTS.md`](AGENTS.md), which points at the
module docstrings that document each one in full. (Every coding agent reads that
file; `CLAUDE.md` is a two-line stub that imports it.)

## Releasing

Images are published to GHCR by `.github/workflows/publish.yml` as a single
multi-architecture package, `ghcr.io/gamso/petkit-local`. That is what the
add-on documentation asks for — a per-architecture name with `{arch}` in it is
described there as a compatibility fallback — and it is also the only shape
docker-compose can use, since Compose has no `{arch}` substitution.

`addon/config.yaml`'s `image:` key is what makes the Supervisor pull that
package instead of building the Dockerfile on the user's machine. It pulls
`<image>:<version>`, taking the version from the same file.

**A push to `main` publishes nothing.** To try a change before it ships, run
`publish` by hand (Actions -> publish -> Run workflow) on that branch: it builds
the branch and moves `dev` to it. To pin one particular build, use its digest —
`image@sha256:...`, which is permanent — rather than adding a tag for it;
`org.opencontainers.image.revision` on the image says which commit it came from.

The version tag is what the Supervisor pulls, so it is a promise that the
version is released, and it is cut by a git tag and nothing else. A tag
publishes only after the test suite and the lint pass on that commit.

A `publish` run on a branch then deletes the image `dev` named before it: that
manifest list, the per-platform images and attestations it names, and the
per-platform indexes it was assembled from (recorded in an annotation on the
list). Only those, and never a digest any current tag still reaches; releases
are never touched.

`prune-packages.yml` deletes the versions `dev` leaves behind as it moves. It
runs weekly, and `workflow_dispatch` takes a `dry_run` (on by default) so you
can see what it would remove first. It never deletes from "untagged" alone: a
multi-architecture image is a manifest list whose per-platform children are
themselves untagged, so deleting those would gut the released image. It resolves
every tag and keeps whatever they reference. The workflow refuses a `v*` tag whose
version does not match `addon/config.yaml`, because an image under a version the
Supervisor never asks for is worse than no image.

A release is therefore:

1. One commit that bumps `addon/config.yaml`, `addon/petkit_local/utils/const.py`
   and `addon/pyproject.toml` together (`tests/test_version.py` checks they
   agree) and adds the `## X.Y.Z` section to `addon/CHANGELOG.md`.
2. `git tag vX.Y.Z && git push --tags`, which builds and publishes all three.

A package created by a workflow inherits the repository's visibility, so a
public repo publishes a public package with nothing to click. Verify it anyway
the first time — `docker logout ghcr.io && docker manifest inspect <image>:dev`
— because the Supervisor pulls anonymously and a private package fails every
install with `pull access denied`.

Actions are pinned to commit SHAs rather than tags. `@v6` is a tag its owner can
move, and `publish` runs with `packages: write`; the comment beside each SHA
says which release it is. Resolve the tag rather than trusting that comment when
updating one:

```sh
gh api repos/docker/build-push-action/commits/v6 --jq .sha
```

## House style

Full type hints. Comments that explain *why* rather than restate the code. Match
the file you are editing.
