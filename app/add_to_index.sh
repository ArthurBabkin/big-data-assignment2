#!/bin/bash
cd "$(dirname "$0")"

if [ -z "$1" ]; then
  echo "usage: bash add_to_index.sh /path/to/docid_Title.txt" >&2
  exit 1
fi

LOCAL_FILE="$1"
FILENAME=$(basename "$LOCAL_FILE")

# check filename format: digits_something.txt
if ! echo "$FILENAME" | grep -qE '^[0-9]+_.+\.txt$'; then
  echo "filename must be <doc_id>_<title>.txt" >&2
  exit 1
fi

NAME="${FILENAME%.txt}"
DOC_ID="${NAME%%_*}"
TITLE_RAW="${NAME#*_}"
TITLE="${TITLE_RAW//_/ }"

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -put -f "$LOCAL_FILE" /data/

TEXT=$(cat "$LOCAL_FILE" | tr '\t' ' ' | tr '\n' ' ')

python3 add_to_index.py "$DOC_ID" "$TITLE" "$TEXT"
