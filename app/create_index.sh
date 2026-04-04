#!/bin/bash

# Run from app/ so mapreduce/*.py paths resolve for -file
cd "$(dirname "$0")"

INPUT_PATH="${1:-/input/data}"

echo "Finding hadoop-streaming jar..."
STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming*.jar" 2>/dev/null | head -1)
if [ -z "$STREAMING_JAR" ] || [ ! -f "$STREAMING_JAR" ]; then
  echo "Error: could not find hadoop-streaming*.jar under HADOOP_HOME=$HADOOP_HOME" >&2
  exit 1
fi

echo "Cleaning TF output: /tmp/indexer/tf"
hdfs dfs -rm -r -f /tmp/indexer/tf

echo "Running Pipeline 1 (TF calculation)..."
hadoop jar "$STREAMING_JAR" \
  -input "$INPUT_PATH" \
  -output /tmp/indexer/tf \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py
if [ $? -ne 0 ]; then
  echo "Error: Pipeline 1 (TF calculation) failed." >&2
  exit 1
fi

echo "Cleaning inverted index output: /indexer/index"
hdfs dfs -rm -r -f /indexer/index

echo "Running Pipeline 2 (inverted index)..."
hadoop jar "$STREAMING_JAR" \
  -input /tmp/indexer/tf \
  -output /indexer/index \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -file mapreduce/mapper2.py \
  -file mapreduce/reducer2.py
if [ $? -ne 0 ]; then
  echo "Error: Pipeline 2 (inverted index) failed." >&2
  exit 1
fi

echo "Index build finished successfully (output: /indexer/index)."
