# Event Logs Data Pipeline using Spark
> 
## Problem Description:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

## Summary
To address the problem faced by Sparkify, an ETL pipeline that makes use of Spark has been developed and deployed on AWS EMR to extract Sparkyfy's data from S3, processes it using Spark and loads the processed data back into S3 as a set of dimensional tables. The data lake's purpose is to allow Sparkify's analytics team to continue finding insights in what songs their users are listening to.  

The data for the data lake resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in Sparkify's music streaming app.  

## Table of contents
* [Data and Code](#data-and-code)
* [Prerequisites](#prerequisites)
* [Database structure](#database-structure)
* [Deployment](#deployment)
* [ETL Output](#etl-output)
* [Instructions on running the application](#instructions-on-running-the-application)

## Data and Code
The dataset for the project resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in Sparkify app.
> s3a://udacity-dend/song_data/ - JSON metadata on the songs in Sparkify's music streaming app.  
> s3a://udacity-dend/log_data/ - JSON files containing log events from the Sparkify's music streaming app.  

In addition to the data files, the project workspace includes:
* **etl.py** - reads data from S3, processes that data using Spark, and writes processed data as a set of dimensional tables back to S3
* **dl.cfg** - contains configuration that allows the ETL pipeline to access AWS EMR cluster. 

## Prerequisites
* AWS EMR cluster
* Apache Spark
* configparser
python 3 is needed to run the python scripts.

## Database structure
![ERD image](/songplays_erd.png)

## Deployment
The following command is used to move files from local development machine to AWS EMR cluster:
> `scp -i <aws perm file> <Local-Path> <username>@<EMR-MasterNode-Endpoint>:~/<EMR-path>`

Use this command to move the etl.py and dl.cfg files to EMR cluster.

## ETL Output

The ETL pipeline saves the data into **s3://sparkprojectdata/<table-name>** 

### Fact Table: 
* **Songplays** - records of log data associated with song plays and these are records with **page= NextSong**
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month

### Dimension Table: 
* **users** - users in the app
    * user_id, first_name, last_name, gender, level
* **songs** - songs in music database
    * song_id, title, artist_id, year, duration
* **artists** - artists in music database
    * artist_id, name, location, lattitude, longitude
* **time** - timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday

## Instructions on running the application
* You must have an access to an AWS EMR cluster
* Issue the following command to run the ETL pipeline:
    * `/usr/bin/spark-submit --master yarn ./etl.py`
### Note: 
* This commancd assumes that the spark-submit is on /usr/bin, change if different.
* Ensure that AWS EMR has access to S3