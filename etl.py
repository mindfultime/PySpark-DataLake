# pip install pyspark

import configparser
import os
from dataProcessing import process_song, process_log, check_df, write_to_s3
from star_schema import dim_songs, dim_users, dim_time, dim_artists, fact_songplays
from pyspark.sql import SparkSession


def main():
    """
    This function instantiates the spark app and connects to aws
    :return: spark session
    """
    # instantiate immediately
    # config obj for parsing aws credential
    config = configparser.ConfigParser()

    # reading aws config file
    config.read_file(open('/Users/akshu/.aws/config'))

    # setting environment variable for AWS S3 access.
    os.environ["AWS_ACCESS_KEY_ID"] = config['default']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"] = config['default']['AWS_SECRET_ACCESS_KEY']

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
    song_file = "song_data/A/B/C/*.json"
    all_songs = "*/*/*/*.json"
    song_filepath = "{}{}".format("s3a://udacity-dend/", song_file)

    # setting path for logs from S3 data lake
    log_file = "log_data/2018/*/*.json"
    all_logs = "*/*/*.json"
    log_filepath = "{}{}".format("s3a://udacity-dend/", log_file)

    # setting datafram dictionary for both song and log
    DF = {'song': process_song(spark, song_filepath),
          'log': process_log(spark, log_filepath)}

    # dim_users based on logDF
    dim_users(DF['log'])

    # dim_artists based on songDF
    dim_artists(DF['song'])

    # dim_songs based on songDF
    dim_songs(DF['song'])

    # dim_time
    check_df(dim_time(DF['log']), 'dim_time')

    # fact_songplays
    check_df(fact_songplays(DF), "fact_songplays")

    # writing dim_songs to s3 and partitioning it by song_id and artist_id
    # pDF.write_to_s3(dim_songs, outPath, ("song_id", "artist_id"))

    spark.stop()
