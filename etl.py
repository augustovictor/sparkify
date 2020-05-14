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


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    setup.main()
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()