import re
import sys
import time

from cassandra.cluster import Cluster

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"


def connect():
    for _ in range(10):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            return cluster, session
        except Exception:
            time.sleep(5)
    raise SystemExit("cannot connect to Cassandra")


def tokenize(text):
    return re.findall(r"\b[a-z]+\b", text.lower())


def main():
    if len(sys.argv) < 4:
        print("usage: python3 add_to_index.py <doc_id> <title> <text>", file=sys.stderr)
        sys.exit(1)

    doc_id = sys.argv[1]
    title = sys.argv[2]
    text = sys.argv[3]

    tokens = tokenize(text)
    if not tokens:
        print("no tokens found, skipping", file=sys.stderr)
        sys.exit(1)

    tf = {}
    for word in tokens:
        tf[word] = tf.get(word, 0) + 1
    doc_len = len(tokens)

    cluster, session = connect()

    existing = session.execute(
        "SELECT doc_id FROM doc_stats WHERE doc_id = %s", (doc_id,)
    ).one()
    if existing:
        print("doc %s already in index" % doc_id)
        cluster.shutdown()
        return

    prep_vocab_insert = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    prep_vocab_update = session.prepare(
        "UPDATE vocabulary SET df = ? WHERE term = ?"
    )
    prep_inv = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
    )
    prep_doc = session.prepare(
        "INSERT INTO doc_stats (doc_id, title, length) VALUES (?, ?, ?)"
    )
    prep_corpus = session.prepare(
        "INSERT INTO corpus_stats (stat_key, stat_value) VALUES (?, ?)"
    )

    for term, freq in tf.items():
        exists = session.execute(
            "SELECT df FROM vocabulary WHERE term = %s", (term,)
        ).one()
        if exists:
            session.execute(prep_vocab_update, (exists.df + 1, term))
        else:
            session.execute(prep_vocab_insert, (term, 1))
        session.execute(prep_inv, (term, doc_id, freq))

    session.execute(prep_doc, (doc_id, title, doc_len))

    n_row = session.execute(
        "SELECT stat_value FROM corpus_stats WHERE stat_key = 'N'"
    ).one()
    avg_row = session.execute(
        "SELECT stat_value FROM corpus_stats WHERE stat_key = 'avg_dl'"
    ).one()

    old_n = n_row.stat_value if n_row else 0.0
    old_avg = avg_row.stat_value if avg_row else 0.0
    new_n = old_n + 1.0
    new_avg = (old_avg * old_n + doc_len) / new_n

    session.execute(prep_corpus, ("N", new_n))
    session.execute(prep_corpus, ("avg_dl", new_avg))

    cluster.shutdown()
    print("added doc %s (%s), N=%d" % (doc_id, title, int(new_n)))


if __name__ == "__main__":
    main()
