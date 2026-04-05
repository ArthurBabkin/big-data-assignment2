#!/bin/bash

if [ -z "$1" ]; then
  echo "usage: bash search.sh \"your query\"" >&2
  exit 1
fi

cd "$(dirname "$0")"
source .venv/bin/activate

spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
  query.py "$1"
