#!/usr/bin/env python3
import sys

def flush(word, postings):
    if word is None or not postings:
        return
    df = len(postings)
    rest = "\t".join(postings)
    print("%s\t%s\t%s" % (word, df, rest))

current_word = None
postings = []

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) < 2:
        continue
    word, posting = parts[0], parts[1]
    if current_word is not None and word != current_word:
        flush(current_word, postings)
        postings = []
    current_word = word
    postings.append(posting)

flush(current_word, postings)
