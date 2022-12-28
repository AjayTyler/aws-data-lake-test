import configparser
import os
from datetime import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


#########
# Setup #
#########

# Read config file and set environmental parameters

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


#################
# ETL Functions #
#################

def create_spark_session():
    '''Creates and returns a SparkSession object.'''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Processes song data from input data filepath and saves the artist and
    song data as parquet files in the path specified in output_data.
    '''

    print('Reading song data...')

    # read song data file
    df = spark.read.json(f'{input_data}')

    # extract columns to create songs table
    songs_table = (
        df
        .select(['song_id', 'title', 'artist_id', 'year', 'duration'])
        .dropDuplicates()
    )

    print('Writing artists to parquet file...')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(f'{output_data}/songs.parquet')

    print('Creating temporary view for songs data')

    # create a temporary view for the session so that we don't have to
    # re-read from the data later on with the songsplay table.
    df.select(['song_id', 'title', 'artist_id', 'artist_name']).dropDuplicates().createOrReplaceTempView('sng_art_mapping')

    # extract columns to create artists table
    artists_table = (
        df
        .withColumnRenamed('artist_name', 'name')
        .withColumnRenamed('artist_location', 'location')
        .withColumnRenamed('artist_latitude', 'latitude')
        .withColumnRenamed('artist_longitude', 'longitude')
        .select(['artist_id', 'name', 'location', 'latitude', 'longitude'])
        .dropDuplicates()
    )

    print('Writing artist metadata to parquet file...')

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(f'{output_data}/artists.parquet')

    print('Finished processing song data!')


def process_log_data(spark, input_data, output_data):
    '''Processes log data from input_data filepath and saves the songplay,
    time, and user data as parquet files in the path specified in output_data.
    '''

    # read log data file and rename columns
    df = (
        spark.read.json(f'{input_data}')
        .withColumnRenamed('songId', 'song_id')
        .withColumnRenamed('sessionId', 'session_id')
        .withColumnRenamed('userId', 'user_id')
        .withColumnRenamed('firstName', 'first_name')
        .withColumnRenamed('lastName', 'last_name')
        .withColumnRenamed('userAgent', 'user_agent')
    )

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = (
        df
        .select(['user_id', 'first_name', 'last_name', 'gender', 'level'])
        .dropDuplicates()
    )

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(f'{output_data}/users.parquet')

    # create UDF to make our timestamp column
    get_timestamp = udf(lambda x: dt.fromtimestamp(x / 1000))

    # create start_time column from original timestamp column
    df = (
        df
        .withColumn('start_time', get_timestamp(df.ts))
        .withColumn('year', year('start_time'))
        .withColumn('month', month('start_time'))
    )

    # extract and create columns to create time table
    time_table = (
        df
        .select(['start_time', 'year', 'month'])
        .dropDuplicates()
        .withColumn('hour', hour('start_time'))
        .withColumn('day', dayofmonth('start_time'))
        .withColumn('week', weekofyear('start_time'))
        .withColumn('weekday', dayofweek('start_time'))
    )

    print('Writing time table to parquet file...')

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}/time.parquet')

    print('Loading song and artist id mapping for songplay enrichment...')

    songs_art = spark.sql('select * from sng_art_mapping')

    # join song and artist data to use for songplays table
    songplays = (
        df
        .withColumn('songplay_id', monotonically_increasing_id())
        .join(
            songs_art,
            on = (df.artist == songs_art.artist_name) & (df.song == songs_art.title),
            how = 'inner'
        )
        .select([
            'songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id',
            'session_id', 'location', 'user_agent', 'year', 'month'
        ])
    )

    print('Writing songplays to parquet file...')

    songplays.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}/songplays.parquet')


#################
# MAIN FUNCTION #
#################

def main():
    spark = create_spark_session()

    # You will need to adjust your input / output filepaths to match
    # your setup.

    input_data = "s3a://udacity-dend"
    output_data = "s3://aws-emr-resources-148368298763-us-west-2/sparkify-data"
    
    process_song_data(spark, input_data + '/song_data/*/*/*/*.json', output_data)
    process_log_data(spark, input_data + '/log_data/*/*/*.json', output_data)

    print('Processes complete.')


if __name__ == "__main__":
    main()
