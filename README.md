Sparkify AWS Datawarehouse Project:
====

Summary:
----

The objective of this project is to create a datawarehouse in AWS for analyitics on the use of our service. More specifically, the project will consist on recovering data from multiple data files we have been collecting for some time, which are stored on S3 and inserting them in a Redshift database, which will be created with this purpose. By having all the data readily available in a columnar storeage relational database we will be able to perform a great variety of queries, very efficiently, to make sense of this data and bring value to our company, by better understanding how our service is being used.

Parts of the project:
---

**dl.cfg** this config file contains the AWS keys needed to access S3 storage.

**etl.py:** this file loads, applying schema on read, all the data from the multiple source files in S3 into spark dataframes. Then it will process the data to create the dataframes with the dimensions we want for our tables. Finally it will write the created dataframes in parquet format in S3, partitioned as specified in the rubric (see details below).

Running the code:
---
Open a python console and run the following files in order:

1. etl.py

Note that to run the files we must use a Python console. Within the project workspace, we can easily open one, then we type *%run filename*

Schema Design and ETL Pipeline:
---

The source data is divided in two datasets, stored in S3, called song_data and log_data respectively. The first contains information on the songs, such as the song name, the artist name and the album, and the second one contains data on the events that have taken place within the system (song reproductions) and information on the users which perform these events, such as the users firstname, lastname and gender, the user agent, or the time of the event. The first dataset has been used to build the songs table and the artists table. The second has been used to build the rest of the tables.


**Choice of schema**

The way this data is stored in the data lake has been thought in order to reduce the duplicated information, thus putting in place a star schema with one fact table (songplays) and four dimension tables (user, song, artist and time). The songplays table contains the data related to the reproduction event in the service, and the rest of the tables complement this information with data which would otherwise be recurring related with the song, the user and the artist, which tend to change less often. Timestamps for the events are stored in the time dimension table. The songplays table is connected to all the rest because it contains all of their keys: *start_time*, *user_id*, *song_id* and *artist_id*. The only other table which has a foreign key is the songs table which has the *artist_id* foreign key connecting it to the artists table.

**Staging Dataframes**

These temporary dataframes should store the raw data, before inserting it in the final schema. This allows us to clean and transform data when selecting columns from the staging dataframes to the final ones.

**DATAFRAME staging_events_df:** it contains all the raw data from the **log_data** file in S3, and has the same columns and formats. Some aditional columns such as *start_time*, *datetime*, *year* and *month* have been created at this stage, because they are to be selected by several dataframes later on.

**DATAFRAME staging_songs_df:** it contains all the raw data from the **song_data** file in S3, and has the same columns and formats. A schema called song_schema is enforced upon loading of the corresponding log file.

**Dimension Dataframes:**

They contain mostly the data which would be recurrently stored in an event table, as they don't usually change from event to event. The column names appear in *italics*.

**DATAFRAME users_table:** contains information about the users in the app such as *user_id* (which will the primary key for this table), *first_name*, *last_name*, *gender* and *level* (paid or free use of service).

**DATAFRAME songs_table:** contains information about the songs contained in the music database and include *song_id* (again, the primary key), *title*, *artist_id*, the *year* it was recorded, and its *duration*. This dataframe will be partitioned using columns *year* and *artist_id* when stored.

**DATAFRAME artists_table:** contains information on the artists in the music database: *artist_id* (primary key), *name* of the artist, *location* of the artist, *longitude* and *latitude* of this location.

**DATAFRAME time_table:** contains the timestamps of the records in songplays, but broken up by time units: *start_time* (absolute time in ms of the event and primary key), *hour*, *day*. *week* of the year, *month*, *year*, *weekday*. This dataframe will be partitioned using columns *year* and *month* when stored.


**Fact Dataframe: songplays_table**

We have only one fact dataframe called songplays. This table contains information from the log data associated with song plays (only those events marked with *page*=NextSong). The table has the following columns: *songplay_id*, *start_time*, *user_id*, *level*, *song_id*, *artist_id*, *session_id*, *location* and *user_agent*. The primary key, *songplay_id* is generated when introducing the data in the dataframe by using the row_number() function. This dataframe will be partitioned using columns *year* and *month* when stored, which have been added for this specific purpose.


About the Dataset:
---

The datasets which is used to build as tables are structured as follows:

**song_dataset:**
![song_dataset_sample](/screenshots/song_dataset_sample.png)

**log_dataset:**
![log_dataset_sample](/screenshots/log_dataset_sample.png)

**Dataset Cleaning:**

The dataset is cleaned when loading it into the dimension and dataframes by enforcing that a certain column is not null using the where method when selecting columns from the staging dataframes. Records are forced to be distinct by using the distinct method when loading.