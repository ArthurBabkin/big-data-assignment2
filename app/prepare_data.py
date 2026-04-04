import os
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

df = spark.read.parquet("/a.parquet")
df = df.select(['id', 'title', 'text'])
n = 100
total = df.count()
frac = min(1.0, 100.0 * n / total) if total else 1.0
df = df.sample(withReplacement=False, fraction=frac, seed=42).limit(n)

os.makedirs("data/", exist_ok=True)


def create_doc(row):
    filename = sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    path = "data/" + filename
    with open(path, "w", encoding="utf-8") as f:
        f.write(row['text'])


df.foreach(create_doc)
print("done writing docs")

subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/input"], check=True)
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=False)
subprocess.run(["hdfs", "dfs", "-put", "-f", "data", "/"], check=True)

sc = spark.sparkContext


def path_to_tsv(pair):
    path, content = pair
    base = os.path.basename(path)
    if not base.endswith(".txt"):
        return None
    name = base[:-4]
    pos = name.find("_")
    if pos < 0:
        return None
    doc_id = name[:pos]
    rest = name[pos + 1:]
    title = rest.replace("_", " ")
    text = content.strip().replace("\t", " ").replace("\n", " ")
    if not text:
        return None
    return f"{doc_id}\t{title}\t{text}"


lines = sc.wholeTextFiles("hdfs:///data").map(path_to_tsv).filter(lambda x: x is not None)
lines.coalesce(1).saveAsTextFile("hdfs:///input/data")
print("Done.")
