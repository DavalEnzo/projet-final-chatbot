import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import StringType, StructField, StructType

# 1. Initialisation de la session Spark
spark = SparkSession \
    .builder \
    .appName("KafkaSparkStreaming") \
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

# 4. Définir le schéma du message Kafka si nécessaire (facultatif)
# Supposons que les messages soient sous forme: {"typologie": "...", "value": "..."}
schema = StructType([
    StructField("typologie", StringType(), True),
    StructField("value", StringType(), True)
])

# 5. Transformation des données - Parsing des messages JSON
df_parsed = df_kafka.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

# 6. Transformation des données dans le format désiré jsonify({'message': typologie, 'value': value})
# Ajouter le champ 'message' qui est égal à 'typologie'
df_transformed = df_parsed.withColumn("message", col("typologie"))

# Reformater les colonnes dans un format JSON: jsonify({'message':typologie, 'value': value})
df_jsonified = df_transformed.select(to_json(struct(col("message"), col("value"))).alias("jsonified_data"))

# 7. Écriture des résultats (dans la console ou dans un autre système)
# Ici, on affiche simplement les résultats dans la console
query = df_jsonified.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Pour écrire dans un autre Kafka topic, par exemple:
# query = df_jsonified \
#    .selectExpr("CAST(jsonified_data AS STRING) as value") \
#    .writeStream \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#    .option("topic", "output_topic") \
#    .start()

# 8. Attendre que la stream finisse
query.awaitTermination()
