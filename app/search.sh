#!/bin/bash
set -euo pipefail

if [ -z "${1:-}" ]; then
  echo 'usage: bash search.sh "your query"' >&2
  exit 1
fi

cd "$(dirname "$0")"
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
export PYSPARK_PYTHON=./environment/bin/python

spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives /app/.venv-packed.tar.gz#environment \
  --conf spark.yarn.archive=hdfs:///apps/spark/spark-jars.zip \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 512m \
  --driver-memory 512m \
  query.py "$1"
