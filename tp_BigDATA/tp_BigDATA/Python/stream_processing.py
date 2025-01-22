from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *




spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .config("spark.jars", "C:/BigData/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar") \
    .getOrCreate()



# Schéma des transactions Kafka
transaction_schema = StructType([
    StructField("id_transaction", StringType(), True),
    StructField("type_transaction", StringType(), True),
    StructField("montant", DoubleType(), True),
    StructField("devise", StringType(), True),
    StructField("date", StringType(), True),
    StructField("lieu", StringType(), True),
    StructField("moyen_paiement", StringType(), True),
    StructField("details", StructType([
        StructField("produit", StringType(), True),
        StructField("quantite", IntegerType(), True),
        StructField("prix_unitaire", DoubleType(), True)
    ])),
    StructField("utilisateur", StructType([
        StructField("id_utilisateur", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("adresse", StringType(), True),
        StructField("email", StringType(), True)
    ]))
])

# Traitement des données
def main():
    spark = SparkSession.builder.appName("KafkaSparkProcessing").getOrCreate()

    # Lecture des données depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transaction") \
        .load()

    # Transformation des données
    transaction_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), transaction_schema).alias("data")) \
        .select("data.*")

    # Nettoyage et transformations
    cleaned_df = transaction_df \
        .filter(col("moyen_paiement") != "erreur") \
        .filter(col("utilisateur.adresse").isNotNull()) \
        .withColumn("montant_eur", col("montant") * 0.92) \
        .withColumn("timestamp", to_timestamp(col("date")))

    # Écriture dans Minio au format Parquet
    query = cleaned_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://minio-bucket/transactions/") \
        .option("checkpointLocation", "/tmp/checkpoints/transactions") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
