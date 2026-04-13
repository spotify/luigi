#!/bin/bash
# Run Luigi tests and log results to test_results.log
#
# Usage:
#   ./run_tests.sh            # default: Py312 (.venv)
#   ./run_tests.sh --py39     # Py39 (.venv39)
#   ./run_tests.sh --py312    # Py312 (.venv)  explicit

set -e

# Parse Python version flag
VENV=""
for arg in "$@"; do
    arg_lower="${arg,,}"
    case "$arg_lower" in
        --py39)  VENV=".venv39" ;;
        --py312) VENV=".venv"   ;;
    esac
done

# Activate the selected virtualenv (skip if already active)
if [[ -n "$VENV" && "$VIRTUAL_ENV" != "$(pwd)/$VENV" ]]; then
    source "$VENV/bin/activate"
fi

# Derive label from the active venv path
case "$VIRTUAL_ENV" in
    */.venv39) PY_LABEL="py39"  ;;
    *)         PY_LABEL="py312" ;;
esac

LOG_FILE="test_results_${PY_LABEL}.log"

# Set config path for tests that need it
export LUIGI_CONFIG_PATH=test/testconfig/luigi.cfg

echo "Running Luigi tests ($(python --version 2>&1))..."
echo "Results will be written to $LOG_FILE"

# s3_test.py uses boto (SigV2) which moto v4+ no longer mocks; skip on Py39
S3_IGNORE=""
if [[ "$PY_LABEL" == "py39" ]]; then
    S3_IGNORE="--ignore="test/contrib/s3_test.py
fi

python -m pytest test/ \
    --ignore=test/contrib/_webhdfs_test.py \
    --ignore=test/redshift_test.py \
    --ignore=test/contrib/esindex_test.py \
    --ignore=test/contrib/hadoop_test.py \
    ${S3_IGNORE:+$S3_IGNORE} \
    --ignore=test/contrib/ecs_test.py \
    --ignore=test/contrib/sge_test.py \
    --ignore=test/contrib/bigquery_test.py \
    --ignore=test/contrib/bigquery_gcloud_test.py \
    --ignore=test/contrib/dataproc_test.py \
    --ignore=test/contrib/postgres_test.py \
    --ignore=test/contrib/postgres_with_server_test.py \
    -v \
    2>&1 | tee "$LOG_FILE"

echo ""
echo "Test run complete. Results saved to $LOG_FILE"
