# sparkify-etl

The goal of this project is to provide an ETL pipeline so data from a music stream app can have its generated logs structured to be analyzed

## Dependency requirements
- python3
- docker

## Setup

Postgres database [Docker]
```shell script
docker run -it --rm -p 5432:5432 --name sparkify-db -e POSTGRES_DB=studentdb -e POSTGRES_USER=student -e POSTGRES_PASSWORD=student postgres:alpine
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