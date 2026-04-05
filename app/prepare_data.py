import os
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local[2]") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

os.makedirs("data/", exist_ok=True)

existing_ids = set()
for fname in os.listdir("data/"):
    if fname.endswith(".txt"):
        pos = fname.find("_")
        if pos > 0:
            try:
                existing_ids.add(int(fname[:pos]))
            except ValueError:
                pass

df = spark.read.parquet("/a.parquet")
df = df.select(['id', 'title', 'text'])
if existing_ids:
    df = df.filter(~col('id').isin(list(existing_ids)))
n = 100
total = df.count()
frac = min(1.0, 100.0 * n / total) if total else 1.0
df = df.sample(withReplacement=False, fraction=frac, seed=42).limit(n)


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
