#!/bin/bash

if [ -z "$1" ]; then
  echo "usage: bash search.sh \"your query\"" >&2
  exit 1
fi

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py "$1"
