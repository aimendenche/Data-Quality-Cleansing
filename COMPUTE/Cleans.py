import uuid
from io import BytesIO
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from datetime import datetime
from minio import Minio
from pyspark.sql.functions import col

class sparkminio:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, bucket):
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.bucket = bucket
        self.spark = self.create_spark_session("test")
        self.client = Minio(endpoint=minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)



    def create_spark_session(self, name):
        # Create SparkConf with all necessary configurations
        spark_conf = SparkConf()
        spark_conf.set("spark.master", "local[*]")
        spark_conf.set("spark.driver.memory", "8g")
        spark_conf.set("spark.driver.host", "192.168.0.18")
        spark_conf.set("spark.driver.bindAddress", "192.168.0.18")
        spark_conf.set("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
        spark_conf.set("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
        spark_conf.set("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
        spark_conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        spark_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_conf.set("spark.jars", "/opt/spark/spark-3.3.2-bin-hadoop3/jars/hadoop-aws-3.3.2.jar,"
                                     "/opt/spark/spark-3.3.2-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.1026.jar")
        spark_conf.set("spark.executor.memory", "8g")
        spark_conf.set("spark.memory.fraction", "0.6")
        spark_conf.set("spark.memory.storageFraction", "0.5")
        spark_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


        return SparkSession.builder.config(conf=spark_conf).appName(str(name)).getOrCreate()

    def process_file(self, source_folder):
        json_path = f"s3a://{self.bucket}/{source_folder}"

        df = self.spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(json_path)

        print("Original column names:", df.columns)

        transformed_columns = [c.upper().replace(' ', '_').replace('-', '_').replace('@','_').replace('.', '_') for c in df.columns]

        print("Transformed column names:", transformed_columns)

        df = df.select(
            *[col(c).alias(new_name) for c, new_name in zip(df.columns, transformed_columns)]
        )

        df.show(truncate=False)

        print(df.schema)

        return df
