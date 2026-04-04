#!/usr/bin/env python3
import sys

current = None
total = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if line.startswith("__DOCLEN__"):
        print(line)
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    word, doc_id, cnt = parts[0], parts[1], int(parts[2])
    key = (word, doc_id)
    if key != current:
        if current is not None:
            print("%s\t%s\t%d" % (current[0], current[1], total))
        current = key
        total = cnt
    else:
        total += cnt

if current is not None:
    print("%s\t%s\t%d" % (current[0], current[1], total))
