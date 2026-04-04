#!/bin/bash

source .venv/bin/activate

INPUT_PATH="${1:-/input/data}"

echo "Creating index for: $INPUT_PATH"
bash create_index.sh "$INPUT_PATH"
if [ $? -ne 0 ]; then
  echo "create_index.sh failed" >&2
  exit 1
fi

echo "Storing index in Cassandra..."
bash store_index.sh
if [ $? -ne 0 ]; then
  echo "store_index.sh failed" >&2
  exit 1
fi

echo "Indexing complete."
