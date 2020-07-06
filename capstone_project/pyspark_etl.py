import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, DateType

LOCAL = True


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.driver.memory", "15g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

#news_data = os.path.join(data_path, 'News_filtered.json')

def process_reddit_submissions(spark, data_path):
    reddit_data = os.path.join(data_path, 'RS_filtered.json')

    schema = StructType([
        StructField("num_comments", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("created_utc", IntegerType(), True),
    ])
    
    # read data file
    df = spark.read.schema(schema).json(reddit_data)
    
    df = df.withColumn('date', from_unixtime('created_utc').cast(DateType()))

    # extract columns to create a table
    table = df.select(
        "title",
        "num_comments",
        "date",
        dayofmonth("date").alias("day"),
        month("date").alias("month"),
        year("date").alias("year"),
    )
    # table.show(n=10)
    
    # write table to parquet files partitioned by month and day
    out_path = os.path.join(data_path, 'reddit.parquet')
    table.write \
        .partitionBy("month", "day") \
        .format("parquet") \
        .save(out_path)


def main():
    spark = create_spark_session()
    data_path = "./" if LOCAL else config.get('default', 'S3')
    process_reddit_submissions(spark, data_path)
    # df.limit(2).toPandas()
#     process_song_data(spark, input_data, output_data)
#     process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
