from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType


def process_song(spark, pathInput):
    """
    This function is used for process the song data into a the defined schema and cleaning the data
    accordingly.
    :param spark: spark app session object
    :param pathInput:  input to the song file path in s3
    :return: cleaned song dataframe called songDF
    """

    # deafult schema on load
    """
     |-- artist_id: string (nullable = true)
     |-- artist_latitude: double (nullable = true)
     |-- artist_location: string (nullable = true)
     |-- artist_longitude: double (nullable = true)
     |-- artist_name: string (nullable = true)
     |-- duration: double (nullable = true)
     |-- num_songs: long (nullable = true)
     |-- song_id: string (nullable = true)
     |-- title: string (nullable = true)
     |-- year: long (nullable = true)
    """

    # setting schema to the following
    songSchema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("num_songs", IntegerType(), True),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True)
    ])

    # reading song file and parsing it to song_df
    songDF = spark.read.json(pathInput, schema=songSchema)

    # dropping duplicates from song files
    songDF = songDF.dropDuplicates()

    # get distinct values from songDF
    songDF = songDF.distinct()

    # check_df(songDF, "song_data")
    return songDF


def process_log(spark, pathInput):
    """
    This function is used for process the song data into a the defined schema and cleaning the data
    accordingly.
    :param spark: spark app session object
    :param pathInput:  input to the song file path in s3
    :return: cleaned log dataframe called logDF
    """
    # default schema on load
    """
    |-- artist: string (nullable = true)
    |-- auth: string (nullable = true)
    |-- firstName: string (nullable = true)
    |-- gender: string (nullable = true)
    |-- itemInSession: long (nullable = true)
    |-- lastName: string (nullable = true)
    |-- length: double (nullable = true)
    |-- level: string (nullable = true)
    |-- location: string (nullable = true)
    |-- method: string (nullable = true)
    |-- page: string (nullable = true)
    |-- registration: double (nullable = true)
    |-- sessionId: long (nullable = true)
    |-- song: string (nullable = true)
    |-- status: long (nullable = true)
    |-- ts: long (nullable = true)
    |-- userAgent: string (nullable = true)
    |-- userId: string (nullable = true)
    """

    # setting schema for log
    logSchema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])

    # reading song file and parsing it to song_df
    logDF = spark.read.json(pathInput, schema=logSchema)

    # dropping duplicates from log file
    logDF = logDF.dropDuplicates()

    # get distinct values from logDF
    logDF = logDF.distinct()

    # filter page based on NextSong
    logDF = logDF.filter(logDF.page == "NextSong").filter(logDF.userId.isNotNull())

    # converting nano epoch to timestamp in column start_time
    logDF = logDF.withColumn("start_time", (logDF.ts / 1000).cast("timestamp"))

    return logDF


def check_df(df, outPath):
    # checking dataframe details
    print("\n"
          "--------------------------------------")
    print("Print Schema for {}".format(outPath))
    df.printSchema()

    print("Showing Dataframe for {}".format(outPath))
    df.show(truncate=False)

    print("printing count of {}".format(outPath))
    print(df.count())

    # drop duplicates in the dataframe
    df.dropDuplicates()
    print("printing {} after dropping duplicates".format(outPath))
    print(df.count())

    print("printing distinct of {}".format(outPath))
    print(df.distinct().count())

    print("\n"
          "--------------------------------------")


def write_to_s3(df, outPath, partitionByCol=None):
    """
    :param df: dataframe from the dim and fact tables
    :param outPath: filename for the dim and fact tables
    :param partitionBy: partition criteria for parquet
    :return: none
    """
    file_path = "{}{}".format(outPath['S3'], outPath['table'])
    if partitionByCol is not None:

        print(" Partitioning BY {0} and writing {2} table to S3 Bucket: {1} datalake"
              .format(partitionByCol, outPath['S3'], outPath['table'])
              )

        df.write.partitionBy(partitionByCol).parquet(file_path).mode("overwrite")

    else:

        print(" Writing {1} table to S3 Bucket: {0} datalake".
              format(outPath['S3'], outPath['table']).mode("overwrite")
              )

        df.write.parquet(file_path)

    print("Completed Writing {} table to S3 Bucket {} for "
          "\n------------------------------------------\n".format(outPath['table'], outPath['S3']))

