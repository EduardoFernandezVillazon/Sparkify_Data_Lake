import configparser
from datetime import datetime
import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import row_number
from pyspark.sql.functions import from_unixtime

def create_spark_session():
    
    """This function creates the spark session instance with the desired configuration"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """ This function creates a staging dataframe for the song data from which the rest of dataframes depending on this data will be created. It also creates these dataframes and stores them in parquet format in a given location."""
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # create schema and read song data file to create staging dataframe
    song_schema = StructType([
    StructField("artist_id", StringType()),
    StructField("artist_latitude", DoubleType()),
    StructField("artist_longitude", DoubleType()),
    StructField("artist_location", StringType()),
    StructField("artist_name", StringType()),
    StructField("duration", DoubleType()),
    StructField("num_songs", IntegerType()),
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("year", IntegerType())
    ])
    
    staging_songs_df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = staging_songs_df.select("song_id","title","artist_id","year","duration").where("song_id is not null").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path = os.path.join(output_data, 'songs/')
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = staging_songs_df.selectExpr("artist_id","artist_name as name", "artist_location as location", "artist_latitude as latitude", \
                                  "artist_longitude as longitude").where("artist_id is not null").distinct()
    
    # write artists table to parquet files
    artists_table_path = os.path.join(output_data, 'artists/')
    artists_table.write.parquet(artists_table_path)


def process_log_data(spark, input_data, output_data):
    """ This function creates a staging dataframe for the log data from which the rest of dataframes depending on this data will be created. It also creates these dataframes and stores them in parquet format in a given location."""
    
    # get filepath to log data file and create staging dataframe
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    staging_events_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    staging_events_df = staging_events_df.where(staging_events_df["page"]=="NextSong")
    
    # create timestamp, datetime, year and month columns in the staging dataframe
    staging_events_df = staging_events_df.withColumn("timestamp", from_unixtime(staging_events_df["ts"]/1000.00))\
                                        .withColumn( "datetime", from_unixtime(staging_events_df["ts"]/1000.00).cast(TimestampType()))
    
    staging_events_df = staging_events_df.withColumn("year", year(staging_events_df["datetime"]))\
                                        .withColumn("month", month(staging_events_df["datetime"]))
    
    # extract columns for users table    
    users_table = staging_events_df.selectExpr("userId as user_id", "firstName as first_name",\
                                               "lastName as last_name", "gender", "level")\
                                                .where("user_id is not null").distinct()

    # write users table to parquet files
    users_table_path = os.path.join(output_data, 'users/')
    users_table.write.parquet(users_table_path)

    # extract columns to create time table
    time_table = staging_events_df.selectExpr("timestamp as start_time","datetime",\
                                              "year","month").where("start_time is not null")
    
    # create new columns needed for time_table
    time_table = time_table.withColumn("week", weekofyear(staging_events_df["datetime"]))\
                            .withColumn("day", dayofmonth(staging_events_df["datetime"]))\
                            .withColumn("hour", hour(staging_events_df["datetime"]))\
                            .withColumn("day_of_week", dayofweek(staging_events_df["datetime"]))
    
    time_table = time_table.drop("datetime")
    
    # write time table to parquet files partitioned by year and month
    time_table_path = os.path.join(output_data, 'time/')
    time_table.write.partitionBy("year", "month").parquet(time_table_path)

    # read in song data to use for songplays table
    
    song_df = spark.read.parquet(songs_table_path) 
    song_df = song_df.select("title","song_id","artist_id")
    
    # extract columns from joined song and log datasets to create songplays table 
    
    songplays_table = staging_events_df.selectExpr("timestamp as start_time", "userId as user_id", "level", "song", \
                                    "sessionId as session_id", "location", "userAgent as user_agent", "year", "month")
    
    # create songplay_id column using the row_numer function
    songplays_table = songplays_table.withColumn("songplay_id", row_number().over(Window.orderBy("start_time")))
    
    # perform join
    songplays_table = songplays_table.join(song_df, songplays_table["song"] == song_df["title"], "leftouter")
    songplays_table = songplays_table.drop("song","title")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = os.path.join(output_data, 'songplays/')
    songplays_table.write.partitionBy("year", "month").parquet(songplays_table_path)


def main():
    """ The main function in this program creates the spark session needed for the program and calls the other functions needed"""
    
	spark = create_spark_session()
	input_data = "s3a://{}:{}@udacity-dend/".format(os.environ['AWS_ACCESS_KEY_ID'] , os.environ['AWS_SECRET_ACCESS_KEY'])
	output_data = "s3a://{}:{}@sparkify-udacity-project-efv/".format(os.environ['AWS_ACCESS_KEY_ID'] , os.environ['AWS_SECRET_ACCESS_KEY'])
	process_song_data(spark, input_data, output_data)    
	process_log_data(spark, input_data, output_data)
	spark.stop()

if __name__ == "__main__":
	config = configparser.ConfigParser()
	config.read('dl.cfg')
	os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
	os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
	main()

