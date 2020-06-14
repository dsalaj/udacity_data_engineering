import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, DateType

LOCAL = False

if LOCAL:
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*')

    # define schema
    schema = StructType([
        StructField("num_songs", IntegerType(), True),  # unused field (not exported to parquet tables)
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), True),
    ])
    
    # read song data file
    df = spark.read.schema(schema).json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    out_path = os.path.join(output_data, 'song_year_artist.parquet') #"data/test_table/key=1")
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .format("parquet") \
        .save(out_path)
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    out_path = os.path.join(output_data, 'artist.parquet')
    artists_table.write \
        .format("parquet") \
        .save(out_path)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data')
    
    # define schema
    schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("last_name", StringType(), True),
        StructField("length", DoubleType(), True),  # ?
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("session_id", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("user_id", StringType(), True),
    ])

    # read log data file
    df = spark.read.schema(schema).json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select("user_id", "first_name", "last_name", "gender", "level")
    
    # write users table to parquet files
    out_path = os.path.join(output_data, 'users.parquet')
    users_table.write \
        .format("parquet") \
        .save(out_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts, TimestampType())
    df = df.withColumn("ts", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: to_timestamp(ts) if ts is not None else None, DateType())
    df = df.withColumn("datetime", get_datetime("ts"))

    # extract columns to create time table
    time_table = df.select("datetime").distinct().select(
        col("datetime").alias("start_time"),
        hour("datetime").alias("hour"),
        dayofmonth("datetime").alias("day"),
        weekofyear("datetime").alias("week"),
        month("datetime").alias("month"),
        year("datetime").alias("year"),
        weekofyear("datetime").alias("weekday"),
    )

    # write time table to parquet files partitioned by year and month
    out_path = os.path.join(output_data, 'time.parquet')
    time_table.write \
        .partitionBy("year", "month") \
        .format("parquet") \
        .save(out_path)

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'song_year_artist.parquet'))

    # extract columns from joined song and log datasets to create songplays table
    joined_data = df.join(song_df, df.song == song_df.title, how='left')
    songplays_table = joined_data.select(
        col("datetime").alias("start_time"),
        "user_id",
        "level",
        "song_id",
        "artist_id",
        "session_id",
        "location",
        "user_agent"
    ).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files
    out_path = os.path.join(output_data, 'songplays.parquet')
    songplays_table.write \
        .format("parquet") \
        .save(out_path)


def main():
    spark = create_spark_session()
    input_data = "./data/" if LOCAL else "s3a://udacity-dend/"
    output_data = "./output/" if LOCAL else "s3a://udacity-s3-emr/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
