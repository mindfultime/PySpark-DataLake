import pyspark.sql.functions as F


def dim_users(df):
    """
    This function is used for processing df into a user dimension table called:
            dim_user: user_id, first_name, last_name, gender, level

        :param df: log dataframe
        :return: dim_users table
        """

    # extracting distinct columns from logDF to users dimensional table
    dim_users = df.select(F.col("userId").alias("user_id"),
                          F.col("firstName").alias("first_name"),
                          F.col("lastName").alias("last_name"),
                          "gender",
                          "level").distinct()

    return dim_users


def dim_artists(df):
    """
        This function is used for process df into artists dimension table called:
            dim_artists: artist_id, name, location, latitude, longitude

        :param df: song dataframe
        :return: dim_artist table
        """
    # extracting columns from logDF to artists dimensional table
    dim_artists = df.select(F.col("artist_id"),
                            F.col("title").alias("song"),
                            F.col("artist_location").alias("location"),
                            F.col("artist_latitude").alias("latitude"),
                            F.col("artist_longitude").alias("longitude")).distinct()

    return dim_artists


def dim_songs(df):
    """
        This function is used for process df into songs dimension table called:
            dim_song: song_id, title, artist_id, year, duration

        :param df: song dataframe
        :return: dim_songs table
        """
    # extracting columns from songDf to songs dimensional table
    dim_songs = df.select("song_id",
                          F.col("title").alias("song"),
                          "artist_id",
                          "year",
                          "duration").distinct()

    return dim_songs


def dim_time(df):
    """
        This function is used for process df into time dimension table called:
            dim_time: start_time, hour, day, week, month, year, weekday

        :param df: log dataframe
        :return: dim_time table
        """
    dim_time = df.select(F.col("start_time"),
                         F.hour(F.col("start_time").alias("hour")),
                         F.dayofmonth(F.col("start_time").alias("day")),
                         F.weekofyear(F.col("start_time").alias("week")),
                         F.month(F.col("start_time").alias("month")),
                         F.year(F.col("start_time").alias("year")),
                         F.date_format(F.col("start_time"), 'F').alias("weekday")
                         ).distinct()

    return dim_time


def fact_songplays(df):
    """
        This function is used for process df into songplays fact table called:
            songplays:  songplay_id, start_time, user_id, level, song_id, artist_id,
                        session_id, session_id, user_agent

        :param df: dataframe dict for song and log
        :return: fact_songplays table
    """

    # joining columns from logDF and songDF to songplays df
    songDF = df['song']
    logDF = df['log']
    songPlays = songDF.join(logDF, songDF.artist_name == logDF.artist) \
        .withColumn("songplay_id", F.monotonically_increasing_id())

    # extracting data into songplays fact table
    fact_songplays = songPlays.select(F.col("songplay_id"),
                                      F.col("start_time"),
                                      F.col("userID").alias("user_id"),
                                      F.col("level"),
                                      F.col("song_id"),
                                      F.col("artist_id"),
                                      F.col("sessionId").alias("session_id"),
                                      F.col("userAgent").alias("user_agent")
                                      )
    return fact_songplays
