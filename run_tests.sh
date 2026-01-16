#!/bin/bash
# Run Luigi tests and log results to test_results.log

set -e

# Activate virtual environment if not already active
if [[ -z "$VIRTUAL_ENV" ]]; then
    source .venv/bin/activate
fi

# Set config path for tests that need it
export LUIGI_CONFIG_PATH=test/testconfig/luigi.cfg

# Run tests and capture output
echo "Running Luigi tests..."
echo "Results will be written to test_results.log"

python -m pytest test/ \
    --ignore=test/contrib/_webhdfs_test.py \
    --ignore=test/redshift_test.py \
    --ignore=test/contrib/esindex_test.py \
    --ignore=test/contrib/hadoop_test.py \
    --ignore=test/contrib/s3_test.py \
    --ignore=test/contrib/ecs_test.py \
    --ignore=test/contrib/sge_test.py \
    --ignore=test/contrib/bigquery_test.py \
    --ignore=test/contrib/bigquery_gcloud_test.py \
    --ignore=test/contrib/dataproc_test.py \
    --ignore=test/contrib/postgres_test.py \
    --ignore=test/contrib/postgres_with_server_test.py \
    -v \
    2>&1 | tee test_results.log

echo ""
echo "Test run complete. Results saved to test_results.log"
