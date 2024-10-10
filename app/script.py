#lire mon csv et le stocker dans mon hdfs avec spark
from pyspark.sql import SparkSession
from pyhive import hive
from pyspark.sql.functions import col, avg, date_format, lag
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.appName("myapp").getOrCreate()

# Read the CSV file
df = spark.read.csv("./train.csv", header=True)

# #lire mon csv et le stocker dans mon hdfs avec spark sauf si je l'ai deja insert
try:
    df = spark.read.parquet("hdfs://namenode:9000/model/train.parquet")
except:
    df = spark.read.csv("./train.csv", header=True)
    df.write.parquet("hdfs://namenode:9000/model/train.parquet")

#afficher le cotenu dans le hdfs
print("Affichage du contenu du fichier train.parquet")
df = spark.read.parquet("hdfs://namenode:9000/model/train.parquet")
df.show()

# Stop the SparkSession
spark.stop()