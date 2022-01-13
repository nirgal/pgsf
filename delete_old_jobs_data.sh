#!/bin/sh

# Directory where pgsf is installed:
PGSF_DIR="$(dirname $0)"

DAYS_TO_KEEP=7

JOB_DIB=$(cd "$PGSF_DIR" && python3 -c "import config; print(config.JOB_DIR)")

echo "Deleting $(cd "$PGSF_DIR" && find "$JOB_DIB" -mtime "+$DAYS_TO_KEEP" -type f | wc -l) files."
cd "$PGSF_DIR" && find "$BASE_DIR/$JOB_DIB" -mtime "+$DAYS_TO_KEEP" -type f -delete
