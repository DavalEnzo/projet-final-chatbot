from pyhive import hive
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import StringType, StructField, StructType
import json

# 1. Initialisation de la session Spark
spark = SparkSession \
    .builder \
    .appName("KafkaSparkStreaming") \
    .enableHiveSupport() \
    .getOrCreate()

hive_host = 'hive-server'
hive_port = 10000
hive_database = 'message_bot'

# Create a connection to Hive
conn = hive.Connection(
    host=hive_host,
    port=hive_port,
    database=hive_database
)

# Configuration des logs
spark.sparkContext.setLogLevel("WARN")

# 2. Définir les configurations de Kafka
kafka_bootstrap_servers = "kafka:9093"  # Adresse du broker Kafka
kafka_topic = "topic1"  # Le topic Kafka à écouter

schema = StructType([
    StructField("message", StringType(), True),
    StructField("value", StringType(), True)
])

# 3. Lecture du flux Kafka en streaming
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Create a cursor
cursor = conn.cursor()

message_df = df_kafka.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

df_jsonified = message_df.select(to_json(struct(col("message"), col("value"))).alias("jsonified_data"))

create_table_query = """
CREATE TABLE IF NOT EXISTS comments (
    message string,
    value string
)
STORED AS PARQUET
"""
cursor.execute(create_table_query)

insert_query = "INSERT INTO comments (message, value) VALUES (%s, %s)"

def insert_into_hive_table(message, value):
    if message is not None and value is not None:
        cursor.execute(insert_query, (message, value))

def insert_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        for row in batch_df.collect():
            jsonified_data = row['jsonified_data']
            data_dict = json.loads(jsonified_data)
            insert_into_hive_table(data_dict['message'], data_dict['value'])

query = df_jsonified \
    .writeStream \
    .format("hive") \
    .foreachBatch(lambda batch_df, batch_id: insert_batch(batch_df, batch_id)) \
    .start()

# 9. Attente de la terminaison du streaming
query.awaitTermination()

print("User data inserted successfully into Hive table.")