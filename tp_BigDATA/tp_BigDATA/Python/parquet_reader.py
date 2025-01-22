from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

    # Lecture des fichiers Parquet
    df = spark.read.parquet("s3a://minio-bucket/transactions/")

    # Affichage des données
    df.show()
    df.printSchema()

if __name__ == "__main__":
    main()
