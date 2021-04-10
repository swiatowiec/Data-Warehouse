import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender char,
    item_session integer,
    last_name varchar,
    length decimal,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration decimal,
    session_id integer,
    song varchar,
    status integer,
    ts bigint,
    user_agent varchar,
    user_id integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id varchar,
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration decimal,
    year integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id integer identity(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id integer,
    location varchar,
    user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id integer PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar NOT NULL DISTKEY,
    year integer,
    duration decimal
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL SORTKEY,
    location varchar,
    latitude decimal,
    longitude decimal
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time  (
    start_time timestamp NOT NULL PRIMARY KEY SORTKEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    json {};
    """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    iam_role {}
    json 'auto';
    """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time, e.user_id, e.level, 
    s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong' AND
    e.song =s.title AND
    e.artist = s.artist_name AND
    e.length = s.duration
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT  user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO song(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
    SELECT DISTINCT start_time, extract(hour from start_time), extract(day from start_time),
    extract(week from start_time), extract(month from start_time),
    extract(year from start_time), extract(dayofweek from start_time)
    FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
