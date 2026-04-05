#!/bin/bash

if [ -z "$1" ]; then
  echo "usage: bash search.sh \"your query\"" >&2
  exit 1
fi

cd "$(dirname "$0")"
source .venv/bin/activate

spark-submit --master local[*] query.py "$1"
