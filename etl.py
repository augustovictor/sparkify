import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import create_tables as setup
import io
import numpy as np
from create_tables import *

DEFAULT_NA_REPLACER = np.nan

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # clean data
    df.replace('', np.nan, inplace=True)
    df.fillna(np.nan, inplace=True)
    df.dropna(axis='index', how='any', subset=['artist_id', 'song_id', 'duration'], inplace=True)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
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
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
#     song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
#     output = io.StringIo()
#     song_data.to_csv(output, sep='\t', header=False, index=False)
#     output.seek(0)
#     cur.copy_from(output, 'song')


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # clean data
    df.fillna(np.nan, inplace=True)
    df.dropna(axis='index', how='any', subset=['artist', 'length', 'song'], inplace=True)

    # filter by NextSong action
    filt_next_song = (df['page'] == 'NextSong')
    df = df[filt_next_song]
    

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday], axis=1)
    column_labels = ("start_time", "hour", "day", "weekofyear", "month", "year", "weekday")
    time_df = pd.DataFrame(data=time_data.values, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            
            # insert songplay record
            songplay_data = songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
        else:
            songid, artistid = None, None


def process_data(cur, conn, filepath, func, filepath_pattern):
    """
    Orchestrates processing of files found in a given filepath
    """

    all_files = _get_files_in(filepath, filepath_pattern)

    total_files_found = len(all_files)

    _print_files_count_for(filepath, total_files_found)

    _process_files(all_files, conn, cur, func, total_files_found)


def _process_files(all_files, conn, cur, func, num_files):
    """
    Processes given files
    """

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        _print_processing_progress(i, num_files)


def _print_processing_progress(i, num_files):
    """
    Prints processing progress
    """

    print('{}/{} files processed.'.format(i, num_files))


def _print_files_count_for(filepath, num_files):
    """
    Encapsulates how found filepaths are formatted and printed
    """

    print('{} files found in {}'.format(num_files, filepath))


def _get_files_in(filepath, filepath_pattern):
    """
    Returns all file paths that match a given pattern
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, filepath_pattern))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def main():
    """
    - Runs create_tables.py(DDLs) script to reset database state
    - Manages a connection with the database
    - Processes data files
    """
    setup.main()
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file, filepath_pattern="*.json")
    process_data(cur, conn, filepath='data/log_data', func=process_log_file, filepath_pattern="*.json")

    conn.close()


if __name__ == "__main__":
    main()