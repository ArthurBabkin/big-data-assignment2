import time

from pyspark.sql import SparkSession
from cassandra.cluster import Cluster


def connect_cassandra():
    cluster = None
    for _ in range(10):
        try:
            cluster = Cluster(["cassandra-server"])
            session = cluster.connect()
            return cluster, session
        except Exception:
            if cluster is not None:
                try:
                    cluster.shutdown()
                except Exception:
                    pass
                cluster = None
            time.sleep(5)
    raise SystemExit("Cannot connect to Cassandra after 10 tries")


def insert_index_partition(rows):
    from cassandra.cluster import Cluster

    cluster = None
    session = None
    try:
        cluster = Cluster(["cassandra-server"])
        session = cluster.connect("search_engine")
        prep_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
        prep_inv = session.prepare(
            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
        )
        for line in rows:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            term = parts[0]
            df = int(parts[1])
            session.execute(prep_vocab, (term, df))
            for pair in parts[2:]:
                if ":" not in pair:
                    continue
                doc_id, tf_str = pair.split(":", 1)
                session.execute(prep_inv, (term, doc_id, int(tf_str)))
    finally:
        if cluster is not None:
            cluster.shutdown()


def insert_doc_stats_partition(rows):
    from cassandra.cluster import Cluster

    cluster = None
    try:
        cluster = Cluster(["cassandra-server"])
        session = cluster.connect("search_engine")
        prep_doc = session.prepare(
            "INSERT INTO doc_stats (doc_id, title, length) VALUES (?, ?, ?)"
        )
        for line in rows:
            line = line.strip()
            if not line.startswith("__DOCLEN__"):
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            doc_id = parts[1]
            title = parts[2]
            length = int(parts[3])
            session.execute(prep_doc, (doc_id, title, length))
    finally:
        if cluster is not None:
            cluster.shutdown()


def main():
    spark = SparkSession.builder.master("local").appName("store_index").getOrCreate()
    sc = spark.sparkContext

    cluster, session = connect_cassandra()
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS search_engine
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        session.set_keyspace("search_engine")
        session.execute(
            "CREATE TABLE IF NOT EXISTS vocabulary (term TEXT PRIMARY KEY, df INT)"
        )
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS inverted_index
            (term TEXT, doc_id TEXT, tf INT, PRIMARY KEY (term, doc_id))
            """
        )
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS doc_stats
            (doc_id TEXT PRIMARY KEY, title TEXT, length INT)
            """
        )
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS corpus_stats
            (stat_key TEXT PRIMARY KEY, stat_value DOUBLE)
            """
        )

        index_rdd = sc.textFile("/indexer/index/part-*")
        index_rdd.foreachPartition(insert_index_partition)
        vocab_size = index_rdd.count()

        doclen_rdd = sc.textFile("/tmp/indexer/tf/part-*").filter(
            lambda x: x.startswith("__DOCLEN__")
        )
        doclen_rdd.foreachPartition(insert_doc_stats_partition)
        lengths = doclen_rdd.map(lambda x: int(x.split("\t")[3])).collect()
        n_docs = len(lengths)
        if n_docs > 0:
            avg_dl = sum(lengths) / float(n_docs)
        else:
            avg_dl = 0.0

        prep_corpus = session.prepare(
            "INSERT INTO corpus_stats (stat_key, stat_value) VALUES (?, ?)"
        )
        session.execute(prep_corpus, ("N", float(n_docs)))
        session.execute(prep_corpus, ("avg_dl", float(avg_dl)))

        print(
            "Stored %d docs, vocabulary size %d, avg_dl %f"
            % (n_docs, vocab_size, avg_dl)
        )
    finally:
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
