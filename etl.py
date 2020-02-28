# pip install pyspark

import configparser
import os
from dataProcessing import process_song, process_log, check_df, write_to_s3
from star_schema import dim_songs, dim_users, dim_time, dim_artists, fact_songplays
from pyspark.sql import SparkSession

# setting configparser for aws and filepaths to instantiate immediately
# config obj for parsing aws credential
config = configparser.ConfigParser()

# reading aws config file
config.read_file(open('/Users/akshu/.aws/config'))

# setting environment variable for AWS S3 access.
os.environ["AWS_ACCESS_KEY_ID"] = config['default']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['default']['AWS_SECRET_ACCESS_KEY']

# getting song and log file path
URL = config['S3']['DEND-URL']
S3 = config['S3']['S3-SPARK']

song_cfg = config['SONG']['SONG_FILE']
log_cfg = config['LOG']['LOG_FILE']


def main():
    """
    This function instantiates the spark app and connects to aws
    :return: spark session
    """
    # starting spark session
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3') \
        .appName('Sparkify') \
        .getOrCreate()

    # setting spark log to display only Error
    spark.sparkContext.setLogLevel("WARN")

    return spark


if __name__ == '__main__':
    # started spark
    spark = main()

    # output s3 bucket
    output = "sparkify"

    # setting path for song from S3 data lake
    song_filepath = "{}{}".format(URL, song_cfg)

    # setting path for logs from S3 data lake
    log_filepath = "{}{}".format(URL, log_cfg)

    # setting datafram dictionary for both song and log
    DF = {'song': process_song(spark, song_filepath),
          'log': process_log(spark, log_filepath)}

    # dim_users based on logDF
    # check_df(dim_users(DF['log']), "dim_users")
    # writing dim_users to s3 partitioned by userId and level
    write_to_s3(dim_users, {'S3': S3, 'table': 'dim_users'}, ("userId", "level"))

    # dim_artists based on songDF
    # check_df(dim_artists(DF['song']), "dim_artists")
    # writing dim_artists to s3 partitioned by song_id and artist_id
    write_to_s3(dim_artists, {'S3': S3, 'table': "dim_artists"}, ("artist_id"))

    # dim_songs based on songDF
    # check_df(dim_songs(DF['song']), "dim_songs")
    # writing dim_songs to s3 partitioned by song_id and artist_id
    write_to_s3(dim_songs, {'S3': S3, 'table': "dim_songs"}, ("year", "artist_id"))

    # dim_time
    # check_df(dim_time(DF['log']), 'dim_time')
    # writing dim_time to s3 partitioned by year and moth
    write_to_s3(dim_time, {'S3': S3, 'table': "dim_time"}, ("year", "month"))

    # fact_songplays
    # check_df(fact_songplays(DF), "fact_songplays")
    # writing fact_songplays to s3 partitioned by year and moth
    write_to_s3(fact_songplays, {'S3': S3, 'table': "fact_songplays"}, ("year", "month"))

    spark.stop()
