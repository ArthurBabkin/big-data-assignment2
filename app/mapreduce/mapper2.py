#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    if line.startswith("__DOCLEN__"):
        continue
    parts = line.split("\t")
    if len(parts) != 3:
        continue
    word, doc_id, tf = parts[0], parts[1], parts[2]
    if not word or not doc_id:
        continue
    print("%s\t%s:%s" % (word, doc_id, tf))
