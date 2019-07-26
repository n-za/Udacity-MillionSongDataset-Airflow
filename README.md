# Million Song Dataset with Redshift and Airflow
Ingesting the Million Song Dataset into Redshift with Airflow

## Purpose of the Project
The code aims at ingesting JSON logs of a streaming service into Redshift. As a scheduler of the loads one uses Airflow.
The coding activity consisted in:
- setting up ad hoc Task in order to favor reuse
- writing SQL commands to do the job

## Contents of the Project

### Load_songs_and_events.py
This is the main file of the project. It provides with all the details of the DAG used for the ELT job.

### Plugin File Helpers/sql_queries
The file contains a class, SqlQueries, more or less used as a namespace that defines several Dictionnaries of SQL commands and subqueries:

* <code> create_queries </code> contains all the <code>CREATE TABLE IF NOT EXISTS</code>
* <code>insert_queries</code> contains all the <code>SELECT</code> statements used for insertion
* <code>not_exists_subqueries</code> contains all the subqueries checking for potential duplicates before insertion
* <code>check_queries</code> contains all the sanity checks one wishes to schedule.
  
### Plugin File operators/stage_redshift.py
The file contains the operator required for COPY statement of JSON records stored in S3 and stores it into the staging tables <code>staging_events</code> and <code>staging_songs</code>

### Plugin File operators/operators/load_fact.py
This file contains the operator that loads the staging table <code> staging_events</code> into the fact table <code>songplays</code>. Note since the assignment demands an hourly schedule and the log file is switched on a daily basis, we load only the facts of the hour of the current <code>execution_time</code>.
The query template used is <code>SqlQueries.insert_queries['songplays']</code> and <code>SqlQueries.not_exists_queries['songplays']</code>

### Plugin File operators/load_dimension.py
This file contains the operator that loads a dimension table, these are <code>songs</code>, <code>users</code>, <code>artists</code>, <code>time</code> based on the data available in the staging tables.
The query template used is <code>SqlQueries.insert_queries[dim]</code> and <code>SqlQueries.not_exists_queries[dim]</code> where <code>dim</code> is the name of the dimension table.

### Plugin File operators/data_quality.py
This file contains the generic operator for sanity checks. The parameters are a valid redshift connection, an SQL commands that returns a count, a predicate function that maps the count to True (success) or False (failure), and the generic args and kwargs parameters.
The collection of sanity checks are stored in <code>SqlQueries.check_queries[check]</code>, where <code>check</code> is the key of the sanity check.

## Usage

### Variables to be defined
The following variables have to defined:
* <code>append_flag</code> when True the dimension information is appended, otherwise the dimension table is first truncated.
* <code>catchup_flag</code> when <code>True</code> the catchup mode of Airflow is activated.
* <code>json_event_format</code> is the S3 path of the format description file for the log data
* <code>json_song_format</code> is the S3 path of the format description for the song data.

### Connection to be defined
* <code>aws_credentials</code> contains the AWS access key id and secret key id.
* <redshift> contains the connection information to the redshift cluster to be used
  
  
# Improvements
## Implemented Improvements
* The project template used the identifier of the source systems to define the primary keys of the tables in the dimensional model. With this approach one needs to discard the records where the identifier is not provided. This approach also prevents us to combine in our dimensional model multiple sources for the raw data. Therefore I have added to the tables of the dimensional model suggorate keys.

## Future Improvements
* The sanity checks could have been put into a sub dag.
* There is a tight coupling between the schedule (hourly) and the SQL command used to load the staging table <code>songplays</code>. 
