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
# Run all tests and log results
./run_tests.sh

# Run specific test file
python -m pytest test/some_test.py -v

# Run specific test
python -m pytest test/some_test.py::TestClass::test_method -v

# Run with config (required for some tests)
LUIGI_CONFIG_PATH=test/testconfig/luigi.cfg python -m pytest test/some_test.py -v
```

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

## Python 3.12 Compatibility Notes
- `random.seed()` no longer accepts tuples - use `hash()` to convert
- `pickle.dump()` requires binary mode (`"wb"`)
- `nose` module uses removed `imp` module - use pytest marks instead
- `logging.config.fileConfig()` raises `FileNotFoundError` for missing files (was `KeyError`)

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

## Common Test Issues
- boto3 tests require AWS region configuration or proper mocking
- SQLAlchemy tests need eager loading for relationships to avoid DetachedInstanceError
- Process-related tests may need small delays for `/proc` filesystem to be ready
