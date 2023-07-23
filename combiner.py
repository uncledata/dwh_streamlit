from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from  pyspark.sql.functions import lit

import glob
import os



def get_spark_session() -> SparkSession:
    return SparkSession.builder.master("local[4]").appName("combiner").getOrCreate()

def read_parquet_files(spark:SparkSession, schema: StructType):
    df = []
    for file in glob.glob("./src/*.parquet"):
        df_new = spark.read.parquet(file, schema=schema).withColumn("filename", lit(file))
        if df:
            df = df.union(df_new)
        else:
            df = df_new

    return df
            

if __name__ == "__main__":
    schema = StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampNTZType(), True),
            StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
        ]
    )

    spark = get_spark_session()

    df = read_parquet_files(spark, schema)
    df.write.mode("overwrite").parquet("./src_adjusted/auto_file")
    