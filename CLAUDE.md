# Luigi Development Guide

## Overview
Luigi is a Python package for building complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, and more.

## Development Setup

### Virtual Environment
```bash
source .venv/bin/activate
```

### Running Tests
```bash
# Run all tests and log results (default: Py312)
./run_tests.sh

# Run against Py39 (.venv39) or Py312 (.venv) explicitly
./run_tests.sh --py39
./run_tests.sh --py312

# Run specific test file
python -m pytest test/some_test.py -v

# Run specific test
python -m pytest test/some_test.py::TestClass::test_method -v

# Run with config (required for some tests)
LUIGI_CONFIG_PATH=test/testconfig/luigi.cfg python -m pytest test/some_test.py -v
```

### Running Race-Condition-Sensitive Tests in Isolation

Some tests use multiprocessing, real network ports, or timing-dependent scheduler state. They pass reliably in isolation but may flake when run alongside the full suite due to port conflicts or shared resources. Run them individually:

```bash
# Multiprocess worker tests (spawn new processes; sensitive to system load)
python -m pytest test/worker_multiprocess_test.py -v

# Dynamic dependency tests with multiple workers (timing-sensitive)
python -m pytest "test/worker_test.py::DynamicDependenciesWithMultipleWorkersTest" -v

# Scheduler tests that start a real server process
python -m pytest test/scheduler_test.py -v

# RPC / server tests (bind to real ports)
python -m pytest test/rpc_test.py test/server_test.py -v

# Remote scheduler tests
python -m pytest test/remote_scheduler_test.py -v
```

If a test fails in the full suite but passes in isolation, it is a pre-existing race condition — not a regression.

**Known macOS-only failures (pass on Linux CI):** `worker_multiprocess_test` and `rpc_test::RequestsFetcherTest::test_fork_changes_session` fail on macOS because Python 3.8+ changed the default multiprocessing start method from `fork` to `spawn`. Spawn requires all subprocess targets to be picklable at the top level, which these tests are not. Do not attempt to fix these locally.

## Project Structure
- `luigi/` - Main package source code
- `test/` - Test files
- `test/contrib/` - Tests for contrib modules (AWS, databases, etc.)
- `test/testconfig/` - Test configuration files

## Key Files
- `luigi/worker.py` - Task execution worker
- `luigi/scheduler.py` - Central scheduler
- `luigi/task.py` - Base Task class
- `luigi/parameter.py` - Parameter types
- `luigi/contrib/` - Integration modules (S3, ECS, Batch, etc.)

## Python 3.9 / 3.12 Dual Compatibility

This branch targets **both Python 3.9 and 3.12**. All changes use Python 3.3+ APIs.

### Py39 Test Environment

```bash
# Create a Py39 virtualenv (requires pyenv 3.9.18 installed)
PYENV_VERSION=3.9.18 python -m venv .venv39
source .venv39/bin/activate
pip install -e ".[toml]"
pip install psutil six sqlalchemy mock boto3 hypothesis pygments  # test deps from tox.ini

# Run the test suite
python -m pytest test/ --ignore=test/contrib/mysqldb_test.py --ignore=test/visualiser \
    --continue-on-collection-errors -x -q 2>&1 | tee /tmp/luigi-test-py39.log
```

### Py312 Compatibility Notes
- `random.seed()` no longer accepts tuples — use `hash()` to convert
- `random.randrange()` no longer accepts floats — use `int(1e10)` instead of `1e10`
- `pickle.dump()` requires binary mode (`"wb"`)
- `pickle.dump()` for scheduler state uses `protocol=3` for cross-version portability
- `collections.Mapping/MutableSet/Iterable` → `collections.abc.*` (removed from top-level in Py312)
- `inspect.getargspec()` → `inspect.getfullargspec()` (removed in Py312)
- `pkg_resources.resource_filename()` → `importlib.resources.files()` (pkg_resources deprecated)
- `nose` module uses removed `imp` module — use pytest marks instead
- `logging.config.fileConfig()` raises `FileNotFoundError` for missing files (was `KeyError`)
- External `six` package (e.g. `from six.moves.urllib...`) → native `urllib.*` (Python 3.0+)
- `six.PY3` checks can be removed entirely — always `True` on any supported Python 3.x

### `IS_LUIGI1_DEPRECATED` flag (boto1 ↔ boto3 dispatch)

`luigi/contrib/_luigi1_compat.py` exposes a single private flag:

- `IS_LUIGI1_DEPRECATED is True`  → `import luigi1` raised. The legacy stack is
  gone, so default to **boto3**.
- `IS_LUIGI1_DEPRECATED is False` → `luigi1` imported cleanly. Keep using the
  legacy **boto1** stack for backwards compatibility.

`luigi/contrib/s3.py` consumes the flag to bind the public aliases:

```python
if IS_LUIGI1_DEPRECATED:
    S3Client = S3ClientBoto3
    ReadableS3File = ReadableS3FileBoto3
else:
    S3Client = S3ClientBoto1
    ReadableS3File = ReadableS3FileBoto1
```

Because `S3Target.__init__` (and the other `client=`-accepting classes —
`S3FlagTarget`, `S3PathTask`, `S3EmrTask`, `S3FlagTask`) fall back to
`S3Client()` when no client is injected, this single dispatch propagates
through the whole S3 surface. Callers that need to pin a specific stack
should import `S3ClientBoto1` / `S3ClientBoto3` (or `ReadableS3FileBoto1` /
`ReadableS3FileBoto3`) directly and pass the client via `client=`.

The flag is evaluated once at module-import time. Modules that branch on it
must be re-imported (or the process restarted) if `luigi1` is added or
removed at runtime.

The `client=` kwarg on `S3PathTask` / `S3EmrTask` / `S3FlagTask` (added in
commit `05c71137` to allow boto3 opt-in) stays. The flag covers the *default*
dispatch; the kwarg covers *explicit* injection — pinning a stack against the
env default, passing custom-credentialled or moto-mocked clients, and
supporting `RedshiftManifestTask`, which forwards `self._client` to its inner
`S3Target`. Downstream callers that previously passed `client=S3ClientBoto3()`
purely to opt into boto3 may drop that argument once `luigi1` is gone — that
cleanup is optional, not forced.

#### Tests for the flag

* `test/contrib/luigi1_compat_test.py` — exercises all three branches of the
  flag's contract: importable (False), unimportable (True via `ImportError`),
  and importable-but-raises (True via non-`ImportError`). Uses
  `sys.modules` stubbing and a custom `sys.meta_path` finder; cleans up in
  `tearDown` so the rest of the suite sees the real flag.
* `test/contrib/s3_test.py::TestFlagDrivenS3Dispatch` — pins the dispatch
  contract: `S3Client` / `ReadableS3File` aliases match the flag, default
  client injection in `S3Target` / `S3PathTask` / `S3EmrTask` / `S3FlagTask`
  uses the flag-resolved class, explicit `client=` overrides the default
  (same-stack always; cross-stack when both backing packages are installed),
  and the aliases flip in both directions when the flag is flipped via
  `importlib.reload`.
* The legacy `try: import boto / HAS_BOTO` setup at the top of `s3_test.py`
  is replaced by:
  - `USING_BOTO1 = S3Client is S3ClientBoto1` — does the flag point at the boto1 stack?
  - `HAS_BOTO_PKG` — is the `boto` package importable?
  - `BOTO1_RUNNABLE = USING_BOTO1 and HAS_BOTO_PKG` — used by the existing skip decorators (renamed from `not HAS_BOTO` to `not BOTO1_RUNNABLE`, same semantics).
  - `S3CLIENT_INSTANTIABLE = HAS_BOTO_PKG if USING_BOTO1 else True` — used as a class-level `@unittest.skipUnless(...)` on `TestS3Target` and `TestS3Client`, and per-test on the dispatch tests that construct `S3Client()`. This handles the degenerate config where `luigi1` is installed (flag flips to False, so `S3Client = S3ClientBoto1`) but `boto` is missing — `S3ClientBoto1.__init__` would raise `ModuleNotFoundError`. We skip rather than fail.
  - `S3ResponseError` is bound to `boto.exception.S3ResponseError` only when `BOTO1_RUNNABLE`; otherwise to `botocore.exceptions.ClientError`. This must follow the *active* stack, not just the *availability* of `boto` — boto can be on path while the flag still selects boto3, in which case errors come from botocore.

#### Scenario matrix (`uv run --no-project --with-editable . ...`)

| # | Python | Stack | luigi1 | Result |
|---|--------|-------|--------|--------|
| 1 | 3.12 | boto + boto3 + moto1 | no | **install fails** — `boto@2.49.0+affirm0` is not py3.12-compatible (`boto.vendored.six.moves` ModuleNotFoundError during build) |
| 2 | 3.12 | boto3 + moto5 | no | 54 pass, 13 skip |
| 3 | 3.9 | boto + boto3 + moto1 | no | 55 pass, 12 skip (cross-stack override runs because boto1 is instantiable) |
| 4 | 3.9 | boto3 + moto1 | no | 55 pass, 12 skip — but `moto==1.3.14` transitively requires `boto`, so this collapses to scenario 3 |
| 5 | 3.9 | boto3 + moto5 | no | 54 pass, 13 skip |
| 6 | 3.9 | boto + boto3 + moto1 | yes | 11 pass, 56 fail — `moto==1.3.14` does not intercept boto1's HTTP calls in this combo, so requests hit real AWS and return `InvalidAccessKeyId`. **Pre-existing test infra issue**, documented by `run_tests.sh` ("s3_test.py uses boto (SigV2) which moto v4+ no longer mocks; skip on Py39"). Not caused by the flag work. |
| 7 | 3.9 | boto3 + moto5 + luigi1 | yes | 4 pass, 63 skip — degenerate config: flag selects boto1 (luigi1 is installed) but `boto` package isn't, so all S3-touching tests skip via `S3CLIENT_INSTANTIABLE`. The 4 dispatch tests that don't instantiate `S3Client` (alias identity, `_readable_file_cls` wiring, reload-based flip test) still pass. |

The scenarios that should run cleanly (2, 3, 5) all do. The scenarios that should fail (1, 7) fail in informative ways — install error on py3.12 boto1, and graceful skip when the flag selects an uninstalled stack. Scenario 6's failures are pre-existing and dodged in normal CI by skipping the file.

### Known Pre-existing Test Failures (not caused by Py312 changes)
- `test/contrib/mysqldb_test.py` — requires MySQL connector not installed in dev env
- `test/visualiser/` — requires Selenium not installed in dev env
- ~39 other failures confirmed pre-existing on both Py39 and Py312 baselines

## Running Luigi

### Quick Test with Local Scheduler (No Server)
For quick testing without starting a server, use `--local-scheduler` which runs an in-memory scheduler:
```bash
# Run the hello world example with in-memory scheduler (no web UI)
# Note: PYTHONPATH=. is needed to find the examples module from project root
PYTHONPATH=. luigi --module examples.hello_world examples.HelloWorldTask --local-scheduler
```

### Central Scheduler with Web UI (luigid)
The `luigid` daemon provides a central scheduler with web interface at http://localhost:8082

#### Run in Foreground
```bash
# Create log directory first
mkdir -p /tmp/luigi-logs

# Start the scheduler with web UI (http://localhost:8082)
luigid --logdir /tmp/luigi-logs

# Or with state persistence (survives restarts)
luigid --port 8082 --logdir /tmp/luigi-logs --state-path /tmp/luigi-state.pickle

# In another terminal, run a task against the central scheduler
PYTHONPATH=. luigi --module examples.hello_world examples.HelloWorldTask
```

#### Run in Background
```bash
mkdir -p /tmp/luigi-logs

# Start scheduler in background
luigid --background --logdir /tmp/luigi-logs --pidfile /tmp/luigi.pid

# Run a task
PYTHONPATH=. luigi --module examples.hello_world examples.HelloWorldTask

# Kill the scheduler
kill $(cat /tmp/luigi.pid)
# Or if pidfile not used
pkill -f luigid
```

## Building and Publishing

### Build the package
```bash
source .venv/bin/activate
python setup.py sdist bdist_wheel
twine check dist/*
```

### Publish to Artifactory
Credentials are in `.pypirc`. Upload using the `pypi-local` index:
```bash
twine upload --config-file .pypirc -r pypi-local dist/*
```

## Common Test Issues
- boto3 tests require AWS region configuration or proper mocking
- SQLAlchemy tests need eager loading for relationships to avoid DetachedInstanceError
- Process-related tests may need small delays for `/proc` filesystem to be ready
