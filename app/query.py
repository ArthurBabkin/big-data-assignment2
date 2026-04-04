#!/usr/bin/env python3
import math
import re
import sys

from cassandra.cluster import Cluster
from pyspark import SparkContext

k1 = 1.0
b = 0.75
CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"


def main():
    if len(sys.argv) < 2:
        print("usage: spark-submit query.py \"search query here\"", file=sys.stderr)
        sys.exit(1)

    query = sys.argv[1]
    terms = set(re.findall(r"\b[a-z]+\b", query.lower()))
    if not terms:
        print("no valid query terms", file=sys.stderr)
        sys.exit(1)

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    n_row = session.execute(
        "SELECT stat_value FROM corpus_stats WHERE stat_key = 'N'"
    ).one()
    avg_row = session.execute(
        "SELECT stat_value FROM corpus_stats WHERE stat_key = 'avg_dl'"
    ).one()
    if n_row is None or avg_row is None:
        print("index stats not found, run index.sh first", file=sys.stderr)
        cluster.shutdown()
        sys.exit(1)

    N = n_row.stat_value
    avg_dl = float(avg_row.stat_value)

    doc_stats = {}
    for drow in session.execute("SELECT doc_id, title, length FROM doc_stats"):
        doc_stats[drow.doc_id] = (drow.title, drow.length)

    sc = SparkContext(appName="bm25_search")
    doc_stats_bc = sc.broadcast(doc_stats)

    def term_postings(term):
        from cassandra.cluster import Cluster

        wcluster = Cluster([CASSANDRA_HOST])
        wsess = wcluster.connect(KEYSPACE)
        try:
            vrow = wsess.execute(
                "SELECT df FROM vocabulary WHERE term = %s", (term,)
            ).one()
            if vrow is None:
                return []
            df = vrow.df
            idf = math.log(float(N) / float(df))
            ds = doc_stats_bc.value
            out = []
            for prow in wsess.execute(
                "SELECT doc_id, tf FROM inverted_index WHERE term = %s", (term,)
            ):
                doc_id = prow.doc_id
                if doc_id not in ds:
                    continue
                dl = float(ds[doc_id][1])
                tf = float(prow.tf)
                denom = k1 * ((1.0 - b) + b * dl / avg_dl) + tf
                score = idf * (k1 + 1.0) * tf / denom
                out.append((doc_id, score))
            return out
        finally:
            wcluster.shutdown()

    terms_list = list(terms)
    rdd = sc.parallelize(terms_list, numSlices=len(terms_list))
    top = (
        rdd.flatMap(term_postings)
        .reduceByKey(lambda a, c: a + c)
        .sortBy(lambda x: -x[1])
        .take(10)
    )

    sc.stop()
    cluster.shutdown()

    print('Top 10 results for: "%s"\n' % query)
    for i, (doc_id, bm25) in enumerate(top, 1):
        title = doc_stats[doc_id][0]
        print(" %d. [%s] %s  (BM25: %.4f)" % (i, doc_id, title, bm25))


if __name__ == "__main__":
    main()
