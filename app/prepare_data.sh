#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "Uploading a.parquet to HDFS..."
hdfs dfs -put -f a.parquet /

echo "Running prepare_data.py..."
spark-submit prepare_data.py

echo "Verifying HDFS /data and /input/data..."
hdfs dfs -count /data
hdfs dfs -count /input/data
echo "Data preparation done."
