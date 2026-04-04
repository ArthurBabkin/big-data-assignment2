#!/bin/bash
echo "Loading index data into Cassandra..."

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

spark-submit store_index.py
