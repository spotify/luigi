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
