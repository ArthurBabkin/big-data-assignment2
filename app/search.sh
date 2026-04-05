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
  --archives /app/.venv.tar.gz#.venv \
  --conf spark.yarn.archive=hdfs:///apps/spark/spark-jars.zip \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=.venv/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=.venv/bin/python \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 512m \
  --driver-memory 512m \
  query.py "$1"
