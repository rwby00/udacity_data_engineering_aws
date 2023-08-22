# Sparkify Song Play Analysis

## Introduction

Sparkify is a startup focusing on providing music streaming services to its users. As the user base grows, there's a pressing need to understand user behavior and preferences. This understanding is crucial for Sparkify to make informed decisions, enhance user experience, and ultimately, grow its business.

## Purpose

The primary purpose of this database is to enable Sparkify to analyze its users' song play behaviors. By structuring the data in a way that's optimized for queries, Sparkify can gain many insights about users

## Database Schema Design

The database uses a **star schema** which is optimized for queries on song play analysis. The schema includes one main fact table (`songplays`), and four dimension tables (`users`, `songs`, `artists`, and `time`).

- **songplays**: Records in event data associated with song plays. Columns include `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, and `user_agent`.
- **users**: Users of the app. Columns include `user_id`, `first_name`, `last_name`, `gender`, and `level`.
- **songs**: Songs in the music database. Columns include `song_id`, `title`, `artist_id`, `year`, and `duration`.
- **artists**: Artists in the music database. Columns include `artist_id`, `name`, `location`, `lattitude`, and `longitude`.
- **time**: Timestamps of records in songplays broken down into specific units. Columns include `start_time`, `hour`, `day`, `week`, `month`, `year`, and `weekday`.

The choice of a star schema provides a few advantages:

1. **Query Performance**: Star schema is denormalized, which means queries require fewer joins, resulting in faster query performance.
2. **User-Friendly**: The simplicity of the star schema makes it more intuitive and accessible to business users.
3. **Scalability**: It's easier to scale and add new dimensions or facts without major changes to the existing schema.

## ETL Pipeline

The ETL pipeline performs the following steps:

1. **Extract**: Data is extracted from S3 buckets. This includes song metadata as well as user activity logs.
2. **Transform**: The data is processed using Python. This involves cleaning the data, transforming timestamps, and structuring it according to the star schema.
3. **Load**: Processed data is loaded into the Redshift database tables.

## Example Queries for Song Play Analysis

1. **Most Played Song**:
   What is the most played song?
   ```sql
    SELECT songs.title, COUNT(songplays.song_id) as play_count
    FROM songplays JOIN songs ON songplays.song_id = songs.song_id
    GROUP BY songs.title
    ORDER BY play_count DESC
    LIMIT 1;
   ```
2. **Peak Usage Time**:
   When is the highest usage time of day by hour for songs
   ```sql
    SELECT time.hour, COUNT(songplays.start_time) as play_count
    FROM songplays JOIN time ON songplays.start_time = time.start_time
    GROUP BY time.hour
    ORDER BY play_count DESC
    LIMIT 1;
   ```
3. **Top Artists by Song Plays:**:
   Who are the top 10 artists based on song play count?
   ```sql
    SELECT artists.name, COUNT(songplays.song_id) as play_count
    FROM songplays JOIN artists ON songplays.artist_id = artists.artist_id
    GROUP BY artists.name
    ORDER BY play_count DESC
    LIMIT 10;
   ```
4. **User Activity by Hour:**:
   How many songs do users play each hour of the day?
   ```sql
    SELECT time.hour, COUNT(songplays.songplay_id) as play_count
    FROM songplays JOIN time ON songplays.start_time = time.start_time
    GROUP BY time.hour
    ORDER BY time.hour;
   ```

5. **User Level Distribution:**:
   How many users are in each subscription level (free vs. paid)?
   ```sql
    SELECT level, COUNT(DISTINCT user_id) as user_count
    FROM users
    GROUP BY level;
   ```

6. **Most Active Users**:
   Who are the top 10 most active users based on song play count?
   ```sql
    SELECT users.first_name, users.last_name, COUNT(songplays.songplay_id) as play_count
    FROM songplays JOIN users ON songplays.user_id = users.user_id
    GROUP BY users.first_name, users.last_name
    ORDER BY play_count DESC
    LIMIT 10;
   ```