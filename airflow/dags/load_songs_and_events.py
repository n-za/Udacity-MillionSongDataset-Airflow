from datetime import datetime, timedelta
import os
from airflow import DAG
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers.sql_queries import SqlQueries
from airflow.models import Variable

# Set the defaults for the Variables
default_append_flag = True
default_catchup_flag = True
# The following two S3 files were made publicly available
default_json_event_format = 's3://nicolas-dend-project3/json_event_format.json'
default_json_song_format = 's3://nicolas-dend-project3/json_song_format.json'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 12, 0, 0, 0, 0),
    'end_date': datetime(2018, 11, 15, 0, 0, 0, 0),
    'depends_on_past': False,
    'email': ['nicolas.zabel@evhr.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': Variable.get('catchup_flag', default_var = default_catchup_flag),
}

dag = DAG('load_songs_and_events',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs = 1,
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-{execution_date.day}-events.json",
    format_json=Variable.get('json_event_format', default_var=default_json_event_format)
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    format_json=Variable.get('json_song_format', default_var=default_json_song_format)
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table='songplays',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table = 'users',
    params = { 'append_flag' : Variable.get('append_flag', default_var=default_append_flag) },
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table = 'songs',
    params = { 'append_flag' : Variable.get('append_flag', default_var=default_append_flag) },
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table = 'artists',
    params = { 'append_flag' : Variable.get('append_flag', default_var=default_append_flag) },
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table = 'time',
    params = { 'append_flag' : Variable.get('append_flag', default_var=default_append_flag) },
    dag=dag
)
# TODO Implement the checks as an SubDag.
run_quality_checks = DummyOperator(task_id='Run_checks_execution',  dag=dag)

run_empty_tables = DataQualityOperator(
    task_id='Empty_tables_test',
    redshift_conn_id="redshift",
    sql_commands = SqlQueries.cardinality_queries,
    check_function = lambda x: x > 0,
    dag=dag,
)

run_dangling_keys = DataQualityOperator(
    task_id='Foreign_Key_violation',
    redshift_conn_id="redshift",
    sql_command = SqlQueries.foreign_key_queries,
    check_function = lambda x: x < 1,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

loading_phase_operator = DummyOperator(task_id='Loading',  dag=dag)


# stage the raw data
start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> loading_phase_operator
# Load dimensions and fact table
loading_phase_operator >> load_user_dimension_table >> load_songplays_table
loading_phase_operator >> load_artist_dimension_table >> load_song_dimension_table >> load_songplays_table 
load_songplays_table >> load_time_dimension_table >> run_quality_checks
# finish with sanity checks
run_quality_checks >> [run_empty_tables, run_dangling_keys] >> end_operator

