
# Project: Data Lake

With the direction from Sparkify to move the data warehouse to data lake, we are implementing this ETL pipline that enables a direct processing of data sources from S3 using Spark and storing the results needed for analytics back to S3.

The data schema still follows the previous star schema to allow for the easy and efficient analytics queries.

## Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong  
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables

- users - users in the app  
`user_id, first_name, last_name, gender, level`
- songs - songs in music database
`song_id, title, artist_id, year, duration`
- artists - artists in music database  
`artist_id, name, location, lattitude, longitude`
- time - timestamps of records in songplays broken down into specific units  
`start_time, hour, day, week, month, year, weekday`

## Deploying on AWS

Make sure to create an EMR cluster with at least three m5.xlarge nodes (1 Master + 2 Core) for executing the `etl.py` script.
On a smaller cluster (like Notebook cluster) the script will fail with:

    org.apache.hadoop.util.DiskChecker$DiskErrorException: No space available in any of the local directories.


