# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS \"user\" CASCADE"
song_table_drop = "DROP TABLE IF EXISTS song CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artist CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR REFERENCES \"user\"(user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES song(song_id),
    artist_id VARCHAR REFERENCES artist(artist_id),
    session_id VARCHAR,
    location VARCHAR,
    user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS \"user\"(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR  PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR REFERENCES artist(artist_id),
    year INTEGER,
    duration DECIMAL(11, 5)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR  PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO \"user\"(user_id, first_name, last_name, gender, level)
    VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING
""")

song_table_insert = ("""
    INSERT INTO song(song_id, title, artist_id, year, duration)
    VALUES(%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artist(artist_id, name, location, latitude, longitude)
    VALUES(%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    VALUES(%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM song s
        JOIN artist a ON s.artist_id = a.artist_id
    WHERE
        s.title = %s
        AND a.name = %s
        AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    user_table_create,
    artist_table_create,
    song_table_create,
    songplay_table_create,
    time_table_create]
drop_table_queries = [
    user_table_drop,
    artist_table_drop,
    song_table_drop,
    songplay_table_drop,
    time_table_drop]
