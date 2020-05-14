import pandas as pd
from sql_queries import *
import create_tables as setup
import numpy as np
from create_tables import *
from file_processor import FileProcessor

DEFAULT_NA_REPLACER = np.nan


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # clean data
    df.replace('', np.nan, inplace=True)
    df.fillna(np.nan, inplace=True)
    df.dropna(
        axis='index',
        how='any',
        subset=[
            'artist_id',
            'song_id',
            'duration'],
        inplace=True)

    # insert artist record
    artist_data = df[['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)

#     artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
#     artist_data.rename(columns={'artist_name': 'name', 'artist_location': 'location', 'artist_latitude': 'latitude'})
#     artist_data.head(0).to_sql('artist', conn, if_exists='append', index=False)
#     artist_data.columns = ('artist_id', 'name', 'location', 'latitude', 'longitude')
#     print(artist_data.columns)
#     output = io.StringIO()
#     artist_data.to_csv(output, sep=',', header=False, index=False)
#     output.seek(0)
#     cur.copy_from(output, 'artist', columns=('artist_id', 'name', 'location', 'latitude', 'longitude'))

#     insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[
        0]
    cur.execute(song_table_insert, song_data)

#     song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
#     output = io.StringIo()
#     song_data.to_csv(output, sep='\t', header=False, index=False)
#     output.seek(0)
#     cur.copy_from(output, 'song')


def process_log_file(cur, filepath):
    """
    Orchestrates log_file processing
    """

    df = pd.read_json(filepath, lines=True)

    df = clean_logfile_data(df)

    df = df[(df['page'] == 'NextSong')]

    insert_datetime(cur, df)

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    insert_users(cur, user_df)

    insert_songplays(cur, df)


def insert_datetime(cur, time_df):
    """
    Persists datetime
    """

    formatted_time_df = format_datetime(time_df)

    for i, row in formatted_time_df.iterrows():
        cur.execute(time_table_insert, list(row))


def clean_logfile_data(df):
    """
    Cleans logfile data to fill nan values with numpy.nan
    Drops rows without necessary attributes for analysis
    """
    return df.fillna(np.nan).dropna(axis='index', how='any',
                                    subset=['artist', 'length', 'song'])


def format_datetime(df):
    """
    Formats datetime
    Returns new DataFrame with "start_time", "hour", "day", "weekofyear", "month", "year", "weekday"
    """

    t = pd.to_datetime(df['ts'], unit='ms')

    time_data = pd.concat(
        [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year,
         t.dt.weekday], axis=1)

    column_labels = (
        "start_time", "hour", "day", "weekofyear", "month", "year", "weekday")

    time_df = pd.DataFrame(data=time_data.values, columns=column_labels)

    return time_df


def insert_users(cur, user_df):
    """
    Inserts users into DB
    """

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


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
    data_file_processor = FileProcessor(
        cur, conn, filepath, func, filepath_pattern)

    all_files = data_file_processor.get_files_in(filepath, filepath_pattern)

    total_files_found = len(all_files)

    data_file_processor.print_files_count_for(filepath, total_files_found)

    data_file_processor.process_files(
        all_files, conn, cur, func, total_files_found)


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
