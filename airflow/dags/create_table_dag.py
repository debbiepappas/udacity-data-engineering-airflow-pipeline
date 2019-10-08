#
import datetime
import logging
from datetime import timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.create_table import CreateTableOperator

from helpers.sql_queries import SqlQueries
# from helpers import SqlQueries

# the following credentials were used in DAG connections to pull in AWS access and secret key
# for the airflow_redshift_user created in IAM users.
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# the access and secret keys are set in the variables below
AWS_KEY = credentials.access_key
AWS_SECRET = credentials.secret_key

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'catchup': False,
    'retry_delay': timedelta(minutes=2)
}

#dag = DAG('create_table_dag',
          #default_args=default_args,
          #start_date = datetime.datetime.now() - datetime.timedelta(days=1),
          #description='Create Tables in Redshift',
          #schedule_interval=None
        #)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
    
create_staging_events_table = CreateTableOperator(
    task_id='Stage_events_create',
    dag=dag,
    table_name = 'staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.staging_events_table_create, 
    provide_context = True
)

create_staging_songs_table = CreateTableOperator(
    task_id='Stage_songs_create',
    dag=dag,
    table_name = 'staging_songs',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.staging_songs_table_create,
    provide_context = True
)


create_songplays_table = CreateTableOperator(
    task_id='Songplays_create',
    dag=dag,
    table_name = 'songplays',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.songplays_table_create,
    provide_context = True
)

create_songs_table = CreateTableOperator(
    task_id='Songs_create',
    dag=dag,
    table_name = 'songs',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.songs_table_create,
    provide_context = True
)

create_users_table = CreateTableOperator(
    task_id='Users_create',
    dag=dag,
    table_name = 'users',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.users_table_create,
    provide_context = True
)

create_artists_table = CreateTableOperator(
    task_id='Artists_create',
    dag=dag,
    table_name = 'artists',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.artists_table_create,
    provide_context = True
)

create_time_table = CreateTableOperator(
    task_id='Time_create',
    dag=dag,
    table_name = 'time',
    redshift_conn_id = 'redshift',
    aws_credentials = {
        'key' : AWS_KEY,
        'secret' : AWS_SECRET
    },
    region = 'us-west-2',
    sql_statement = SqlQueries.time_table_create,
    provide_context = True
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)    
    
start_operator >> create_staging_events_table >> end_operator    
start_operator >> create_staging_songs_table >> end_operator     
start_operator >> create_songplays_table >> end_operator     
start_operator >> create_songs_table >> end_operator     
start_operator >> create_users_table >> end_operator     
start_operator >> create_artists_table >> end_operator     
start_operator >> create_time_table >> end_operator    
       