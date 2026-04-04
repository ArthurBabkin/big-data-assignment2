#!/usr/bin/env python3
import re
import sys

for line in sys.stdin:
    line = line.rstrip("\n")
    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue
    doc_id, title, text = parts
    tokens = re.findall(r"\b[a-z]+\b", text.lower())
    num_words = len(tokens)
    print("__DOCLEN__\t%s\t%s\t%d" % (doc_id, title, num_words))
    for word in tokens:
        print("%s\t%s\t1" % (word, doc_id))
