#Import Modules
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as Tst, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date,TimestampType as t_s
from pyspark.sql.functions import monotonically_increasing_id
#Set config
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY'] 

# def create_spark_session ()
def create_spark_session():
    """
    in this method it will start spark session with org.apache.hadoop:hadoop-aws:2.7.0 packages
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

#def process_song_data()
def process_song_data(spark, input_data, output_data):
    """
    in this method  the reading from the input file and creating the two dataframes using Spark
    , inputs are:
    spark: the spark object
    input_data: the input link
    output_data: the output link
    """
    # get filepath to song data file
    song_data = input_data+"song-data/A/B/B/*.json"
    
    # read song data file
    schema_for_song  = Tst([
    Fld("num_songs",Int()),
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_name",Str()),
    Fld("song_id",Str()),
    Fld("title",Str()),
    Fld("duration",Dbl()),
    Fld("year",Int()),
   
])
    # read song data file
    df= spark.read.json(song_data, schema=schema_for_song)

    # extract columns to create songs table
    tableOFsong = df["song_id", "title", "artist_id", "year", "duration"]
    songs_table = tableOFsong.dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songsFile/', 'overwrite')

    # extract columns to create artists table
    tableOFartist = df["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = tableOFartist.dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artistsFile/', 'overwrite')

# def process_log_data(spark, input_data, output_data)
def process_log_data(spark, input_data, output_data):
    """
     in this method  the reading from the input file and creating the two dataframes using Spark
     , inputs are:
    spark: the spark object
    input_data: the input link
    output_data: the output link
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    tableOFusers = df["userId", "firstName", "lastName", "gender", "level"]
    users_table = tableOFusers.dropDuplicates(['userId'])
    print(users_table.toPandas())
    # write users table to parquet files
    users_table.write.parquet(output_data + 'usersFile/', 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: str(int(int(t)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df["ts"]))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda t: str(datetime.fromtimestamp(int(t) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df["ts"]))
    
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),
                           hour("datetime").alias('hour'),
                           dayofmonth("datetime").alias('day'),
                           weekofyear("datetime").alias('week'),
                           month("datetime").alias('month'),
                           year("datetime").alias('year'),
                           date_format("datetime","u").alias('weekday')
                          ).distinct()
    time_table = time_table.dropDuplicates(['start_time'])
    print(time_table.toPandas())
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'timeFile/', 'overwrite')
    
    
    # read in song data and artist to be use for songplays table
    songs_output = spark.read.parquet(output_data + 'songsFile/*/*/*')

    artists_output = spark.read.parquet(output_data + 'artistsFile/*')
    print(songs_output.toPandas())
    print(artists_output.toPandas())
    # joined song and log datasets to create songplays table 
    songs_info = df.join(songs_output, (df.song == songs_output.title))
    songs_artists_table = songs_info.join(artists_output, (songs_info.artist == artists_output.artist_name))
    print(songs_info.toPandas())
    # extract columns from joined song and log datasets to create songplays table
    songplays = songs_artists_table.join(
        time_table,
        songs_artists_table.datetime == time_table.start_time, 'left'
    )
    print(songplays.toPandas())
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ) 
    print(songplays.toPandas())
    # write songplays table to parquet files
    songplays_table.write.parquet(output_data + 'songplaysFile/')

 #  main()  
def main():
    """
    in this main method all data will be called in the input and the output process with spark too
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-data-lack-work/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

# Start the code
if __name__ == "__main__":
    main()
