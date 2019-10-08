from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator 

from helpers.sql_queries import SqlQueries

# the following credentials were used in DAG connections to pull in AWS access and secret key
# for the airflow_redshift_user created in IAM users.
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# the access and secret keys are set in the variables below
AWS_KEY = credentials.access_key
AWS_SECRET = credentials.secret_key


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 3),
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

#dag = DAG('udac_example_dag',
          #default_args=default_args,
          #description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
        #)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name = 'staging_events',
    redshift_conn_id = 'redshift',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data/2018/11/{ds}-events.json',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name = 'staging_songs',
    redshift_conn_id = 'redshift',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2'
)    



load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    source_table = 'songplays',
    target_table = 'songplays',
    redshift_conn_id = 'redshift',
    append_data = True,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.songplays_table_insert, 
    provide_context = True
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    target_table = 'users',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.users_table_insert, 
    provide_context = True
)


load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    target_table = 'songs',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.songs_table_insert, 
    provide_context = True
)


load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    target_table = 'artists',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.artists_table_insert, 
    provide_context = True
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table = 'time',
    redshift_conn_id = 'redshift',
    append_data = False,
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.time_table_insert, 
    provide_context = True
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    provide_context = True,
    dq_checks = [
        { 'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        { 'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_users_dimension_table >> run_quality_checks  
load_songplays_table >> load_artists_dimension_table >> run_quality_checks 
load_songplays_table >> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_songs_dimension_table >> run_quality_checks
run_quality_checks >> end_operator   

