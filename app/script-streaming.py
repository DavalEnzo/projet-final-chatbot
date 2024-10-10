import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import StringType, StructField, StructType

# 1. Initialisation de la session Spark avec support pour Hive
spark = SparkSession \
    .builder \
    .appName("KafkaSparkStreaming") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration des logs
spark.sparkContext.setLogLevel("WARN")

# 2. Définir les configurations de Kafka
kafka_bootstrap_servers = "localhost:9092"  # Adresse du broker Kafka
kafka_topic = "my_topic"  # Le topic Kafka à écouter

# 3. Lecture du flux Kafka en streaming
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Les données de Kafka sont en binaire, il faut donc les convertir en String
df_kafka = df_kafka.selectExpr("CAST(value AS STRING) as value")

# 4. Définir le schéma du message Kafka (facultatif)
# Supposons que les messages soient sous forme: {"typologie": "...", "value": "..."}
schema = StructType([
    StructField("message", StringType(), True),
    StructField("value", StringType(), True)
])

# 5. Transformation des données - Parsing des messages JSON
df_parsed = df_kafka.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

# 6. Transformation des données dans le format désiré jsonify({'message': typologie, 'value': value})
# Ajouter le champ 'message' qui est égal à 'typologie'
df_transformed = df_parsed.withColumn("message", col("message"))

# Reformater les colonnes dans un format JSON: jsonify({'message': typologie, 'value': value})
df_jsonified = df_transformed.select(col("message"), col("value"))

# 7. Écriture des données dans Hive
# Création d'une table Hive (si elle n'existe pas déjà)
spark.sql("CREATE TABLE IF NOT EXISTS kafka_data (message STRING, value STRING) STORED AS PARQUET")

# Écriture continue des données dans la table Hive
query = df_jsonified.writeStream \
    .outputMode("append") \
    .format("hive") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .option("path", "/user/hive/warehouse/kafka_data") \
    .toTable("kafka_data")  # Nom de la table Hive

# 8. Attendre que la stream finisse
query.awaitTermination()