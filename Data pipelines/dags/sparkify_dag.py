from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='staging_events',
    s3_bucket='haint84',
    s3_key='log-data/',  # adjust accordingly
    json_format='auto'  # or the path to your JSONPath file, if not using 'auto'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='haint84',
    s3_key='song-data/',  # adjust accordingly
    json_format='auto'  # or the path to your JSONPath file, if not using 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',  # your fact table name
    select_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    select_sql=SqlQueries.user_table_insert,
    insert_mode='overwrite'  # or 'append'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    select_sql=SqlQueries.user_table_insert,
    insert_mode='overwrite'  # or 'append'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    select_sql=SqlQueries.user_table_insert,
    insert_mode='overwrite'  # or 'append'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    select_sql=SqlQueries.user_table_insert,
    insert_mode='overwrite'  # or 'append'
)

table_checks = [
    {'table': 'users', 'min_records': 1},
    {'table': 'songs', 'min_records': 1}, 
    {'table': 'artists', 'min_records': 1}, 
    {'table': 'time', 'min_records': 1},
    {'table': 'songplays', 'min_records': 1}
]

dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    table_checks=table_checks,
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
