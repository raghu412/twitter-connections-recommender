import numpy as np
import pickle
import os
import sys
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, LongType, ArrayType, FloatType, IntegerType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.embedding import get_model


bucket_table_path = "storage/delta_tables/bucket_table"
hash_planes_path = "storage/hash_planes.pkl"

spark = SparkSession.builder \
    .appName("ALSH_Tweet_Streaming") \
    .config("spark.sql.streaming.schemaInference", True) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

model = get_model()
bc_model = spark.sparkContext.broadcast(model)

hash_planes = None
if not os.path.exists(hash_planes_path):
    hash_planes = [list(np.random.randn(384)) for _ in range(10)]
    with open(hash_planes_path, 'wb') as f: pickle.dump(hash_planes, f)
else:
    with open(hash_planes_path, 'rb') as f: hash_planes = pickle.load(f)
          
bc_planes = spark.sparkContext.broadcast(hash_planes)

@F.udf(ArrayType(FloatType()))
def embed_text(text): return bc_model.value.encode(text).tolist()

@F.udf(ArrayType(IntegerType()))
def compute_hash(vec): return [int(np.dot(vec, plane) > 0) for plane in bc_planes.value]

@F.udf(FloatType())
def cosine_similarity(vec1, vec2):
    vec1_np = np.array(vec1)
    vec2_np = np.array(vec2)
    norm1 = np.linalg.norm(vec1_np)
    norm2 = np.linalg.norm(vec2_np)
    if norm1 == 0.0 or norm2 == 0.0:
        return float(0.0)
    return float(np.dot(vec1_np, vec2_np) / (norm1 * norm2))

def compute_similarities(cur_bucket_df, batch_id):
    if cur_bucket_df.isEmpty(): return

    print("New batch arrived")
    cur_bucket_df = cur_bucket_df.withColumn("row_id", F.monotonically_increasing_id())
    sim_df = cur_bucket_df.alias("df1").join(cur_bucket_df.alias("df2"),(F.col("df1.hash_key") == F.col("df2.hash_key")) &(F.col("df1.row_id") < F.col("df2.row_id"))) 
    sim_df = sim_df.select(
        F.col("df1.tweet_id").alias("tweet_id_1"),
        F.col("df1.user_id").alias("user_id_1"),
        F.col("df1.embedding").alias("embedding_1"),
        F.col("df2.tweet_id").alias("tweet_id_2"),
        F.col("df2.user_id").alias("user_id_2"),
        F.col("df2.embedding").alias("embedding_2")
    )
    sim_df = sim_df.withColumn("similarity", cosine_similarity(F.col("embedding_1"), F.col("embedding_2")))
    sim_df = sim_df.drop("embedding_1", "embedding_2")

    if DeltaTable.isDeltaTable(spark, bucket_table_path):
        bucket_df = spark.read.format("delta").load(bucket_table_path)
    else: bucket_df = spark.createDataFrame([], cur_bucket_df.schema)

    bucket_df = bucket_df.cache()
    bucket_df.count()

    hash_keys = cur_bucket_df.select("hash_key").distinct().collect()
    for hash_key in hash_keys:
        bucket_filtered_df = bucket_df.filter(F.col("hash_key") == hash_key["hash_key"])
        cur_bucket_filtered_df = cur_bucket_df.filter(F.col("hash_key") == hash_key["hash_key"])

        if not(bucket_filtered_df.isEmpty() or cur_bucket_filtered_df.isEmpty()):
            cur_sim_df = bucket_filtered_df.alias("bucket_filtered").crossJoin(F.broadcast(cur_bucket_df.alias("cur_bucket_filtered")))
            cur_sim_df = cur_sim_df.select(
                F.col("bucket_filtered.tweet_id").alias("tweet_id_1"),
                F.col("bucket_filtered.user_id").alias("user_id_1"),
                F.col("bucket_filtered.embedding").alias("embedding_1"),
                F.col("cur_bucket_filtered.tweet_id").alias("tweet_id_2"),
                F.col("cur_bucket_filtered.user_id").alias("user_id_2"),
                F.col("cur_bucket_filtered.embedding").alias("embedding_2")
            )
            cur_sim_df = cur_sim_df.withColumn("similarity", cosine_similarity(F.col("embedding_1"), F.col("embedding_2")))
            cur_sim_df = cur_sim_df.drop("embedding_1", "embedding_2")
            sim_df = sim_df.union(cur_sim_df)
    
    sim_df.show(truncate=True)

    cur_bucket_df.write.format("delta").mode("append").save(bucket_table_path) 
    if(not sim_df.isEmpty()): 
        #sim_df.write.format("delta").mode("append").save(similarities_table_path) 
        with sqlite3.connect("storage/database.db") as conn:
            sim_df.toPandas().to_sql("similarities", conn, if_exists="append", index=False)
            conn.commit()
    
schema = StructType() \
    .add("user_id", StringType()) \
    .add("tweet_id", LongType()) \
    .add("text", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "posts") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json").select(F.from_json("json", schema).alias("data")).select("data.*")

df = df.withColumn("embedding", embed_text(F.col("text")))
df = df.withColumn("hash_key", compute_hash(F.col("embedding")))
df = df.withColumn("hash_key", F.concat_ws("_", F.col("hash_key")))

query = df.writeStream \
    .foreachBatch(compute_similarities) \
    .option("checkpointLocation", "/tmp/checkpoints/alsh_tweet") \
    .start()
query.awaitTermination()