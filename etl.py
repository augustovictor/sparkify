import pandas as pd

from files_processor import FilesProcessor
from sql_statements import *
import create_tables as setup
import numpy as np
from create_tables import *
from file_paths_appender import FilePathsAppender
from time_formatter import TimeFormatter

DEFAULT_NA_REPLACER = np.nan


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # clean data
    df = get_cleaned_song_data(df)

    artist_data = df[['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude']].values[0]

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[
        0]

    # insert artist record
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    cur.execute(song_table_insert, song_data)


def get_cleaned_song_data(df):
    return df.replace('', DEFAULT_NA_REPLACER)\
        .fillna(DEFAULT_NA_REPLACER)\
        .dropna(
        axis='index',
        how='any',
        subset=[
            'artist_id',
            'song_id',
            'duration'],
    )


def process_log_file(cur, filepath):
    """
    Orchestrates log_file processing
    """

    df = pd.read_json(filepath, lines=True)

    df = clean_logfile_data(df)

    df = df[(df['page'] == 'NextSong')]

    insert_datetime(cur, df)

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    insert_statements(cur, user_df, user_table_insert)

    insert_songplays(cur, df)


def insert_datetime(cur, time_df):
    """
    Persists datetime
    """

    formatted_time_df = TimeFormatter.format_datetime(time_df)

    insert_statements(cur, formatted_time_df, time_table_insert)


def clean_logfile_data(df):
    """
    Cleans logfile data to fill nan values with numpy.nan
    Drops rows without necessary attributes for analysis
    """
    return df.fillna(DEFAULT_NA_REPLACER).dropna(
        axis='index', how='any', subset=[
            'artist', 'length', 'song'])


def insert_statements(cur, df, statement):
    """
    Executes given statement for all rows in provided dataframe
    """

    for i, row in df.iterrows():
        cur.execute(statement, list(row))


def insert_songplays(cur, df):
    """
    Insert songplays into DB
    """

    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results

            # insert songplay record
            songplay_data = (
                pd.to_datetime(
                    row.ts,
                    unit='ms'),
                row.userId,
                row.level,
                songid,
                artistid,
                row.sessionId,
                row.location,
                row.userAgent)

            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func, filepath_pattern):
    """
    Orchestrates processing of files found in a given filepath
    """
    data_file_processor = FilePathsAppender(
        cur, conn, filepath, filepath_pattern)

    all_files = data_file_processor.get_files_in()

    total_files_found = len(all_files)

    data_file_processor.print_files_count_for(filepath, total_files_found)

    FilesProcessor(
        all_files,
        conn,
        cur,
        func,
        total_files_found).process_files()


def main():
    """
    - Runs create_tables.py(DDLs) script to reset database state
    - Manages a connection with the database
    - Processes data files
    """
    setup.main()

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(
        cur,
        conn,
        filepath='data/song_data',
        func=process_song_file,
        filepath_pattern="*.json")
    process_data(
        cur,
        conn,
        filepath='data/log_data',
        func=process_log_file,
        filepath_pattern="*.json")

    conn.close()


if __name__ == "__main__":
    main()
