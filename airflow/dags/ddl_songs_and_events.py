from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 12, 0, 0, 0, 0),
    'end_date': datetime(2018, 11, 15, 0, 0, 0, 0),
    'depends_on_past': False,
    'email': ['nicolas.zabel@evhr.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('ddl_songs_and_events',
          default_args=default_args,
          description='Create tables in Redshift with Airflow',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_songs = PostgresOperator(
    task_id = "create_staging_songs",
    postgres_conn_id = "redshift",
    dag = dag,
    sql = SqlQueries.create_queries['staging_songs']
)
                                    
create_staging_events = PostgresOperator(
    task_id = "create_staging_events",
    postgres_conn_id = "redshift",
    dag = dag,
    sql= SqlQueries.create_queries['staging_events']
)

create_songplays = PostgresOperator(
    task_id = "create_songplays",
    postgres_conn_id = "redshift",
    dag = dag,
    sql= SqlQueries.create_queries['songplays']
)


create_users = PostgresOperator(
    task_id = "create_users",
    postgres_conn_id = "redshift",
    dag = dag,
    sql= SqlQueries.create_queries['users']
)


create_artists = PostgresOperator(
    task_id = "create_artists",
    postgres_conn_id = "redshift",
    dag = dag,
    sql= SqlQueries.create_queries['artists']
)


create_songs = PostgresOperator(
    task_id = "create_songs",
    postgres_conn_id = "redshift",
    dag = dag,
    sql= SqlQueries.create_queries['songs']
)


create_time = PostgresOperator(
    task_id = "create_time",
    postgres_conn_id = "redshift",
    dag = dag,
    sql= SqlQueries.create_queries['time']
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [create_staging_events, create_staging_songs, create_users, create_artists, create_songs, create_time, create_songplays] >> end_operator

