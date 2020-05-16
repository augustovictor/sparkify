# sparkify-etl

The goal of this project is to provide an ETL pipeline so data from a music stream app can have its generated logs structured to be analyzed

## Dependency requirements
- [docker](https://docs.docker.com/engine/install/binaries/)
- [python3](https://realpython.com/installing-python/)

## Running ETL

Postgres database [Docker]
```shell script
make run-db
make etl
```

## Project structure overview
```shell script
├── Makefile # Useful commands
├── README.md # Documentation
├── create_tables.py # Database setup (DDL)
├── etl.ipynb # Jupyter notebook for etl analysis
├── etl.py # Etl pipeline for song data and logs analysis
├── file_paths_appender.py # Fetches all file paths
├── file_processor.py # Processes files
├── requirements.txt # Python dependencies
├── sql_statements.py # Database DML and DQL statements
├──  test.ipynb # Jupyter notebook for ETL results analysis
├──  time_formatter.py # Formats time to "start_time", "hour", "day", "weekofyear", "month", "year", "weekday"
└── data # Sparkify songs and events log data
    ├── log_data # Events log
    │   └── 2018
    │       └── 11
    └── song_data # Songs data
        └── A
            ├── A
            │   ├── A
            │   ├── B
            │   └── C
            └── B
                ├── A
                ├── B
                └── C
```

## Steps

### Extract
There are songs metadata in `data/song_data` directory. Sample: `data/song_data/A/A/C/TRAACCG128F92E8A55.json`

```json
{
    "num_songs": 1,
    "artist_id": "AR5KOSW1187FB35FF4",
    "artist_latitude": 49.80388,
    "artist_longitude": 15.47491,
    "artist_location": "Dubai UAE",
    "artist_name": "Elena",
    "song_id": "SOZCTXZ12AB0182364",
    "title": "Setanta matins",
    "duration": 269.58322,
    "year": 0
}
```

And also generated logs by sparkify app in `directory`. Sample: `data/log_data/2018/11/2018-11-01-events.json`

```json
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
...
```

### Transform

All `None` values were replaced with `numpy.nan`;

Removed rows from logs dataframe with `dropna` where any of the following columns were `nan`: `artist`, `song`, `length`, since those were the columns used to identify `artist` and `song` in postgres tables. This removed ~16% of log events;



### Load

Data is loaded into the following postgres tables:
songplay - Fact table
time - Time dimension table
artist - Artist dimension table
song - Song dimension table
user - User dimension table

### Todo
fillna
Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
Add data quality checks
Create a dashboard for analytic queries on your new database

## Project criteria

### Table creation
- [ ] Table creation script runs without errors: The script, create_tables.py, runs in the terminal without errors. The script successfully connects to the Sparkify database, drops any tables if they exist, and creates the tables.
- [ ] Fact and dimensional tables for a star schema are properly defined: CREATE statements in sql_queries.py specify all columns for each of the five tables with the right data types and conditions

### ETL
- [x] ETL script runs without errors: The script, etl.py, runs in the terminal without errors. The script connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables.
- [x] ETL script properly processes transformations in Python: INSERT statements are correctly written for each table, and handle existing records where appropriate. songs and artists tables are used to retrieve the correct information for the songplays INSERT

### Code Quality
- [x] The project shows proper use of documentation: The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring
- [ ] The project code is clean and modular: Scripts have an intuitive, easy-to-follow structure with code separated into logical functions. Naming for variables and functions follows the PEP8 style guidelines


### Suggestions to Make Your Project Stand Out!

- [ ] Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
- [ ] Add data quality checks
- [ ] Create a dashboard for analytic queries on your new database