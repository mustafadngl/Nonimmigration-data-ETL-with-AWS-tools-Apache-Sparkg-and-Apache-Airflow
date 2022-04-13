from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

S3_BUCKET = ""
S3_REGION = ""
REDSHIFT_CONN_ID = "redshift"
AWS_CREDENTIALS_ID = 'aws_credentials'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016,12,1),
    'depends_on_paste': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}
#In order to check data quality following default 'check_quary' list can be manuplated in order to sql query and expectation value.
check_query=[
        {"check_sql":"SELECT COUNT(*) FROM facti94 WHERE facti94_id IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM date WHERE arrival_date IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM ports WHERE port_id IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM demographics WHERE demog_id IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM citytemperatures WHERE temp_id IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM personali94 WHERE record_id IS NULL","expected_value":0}
    ]
#In order to check length and quantity via table names are listed. 
table_names=["facti94","date","ports","demographics","citytemperatures","personali94"]

dag = DAG('capstone_dag',
          default_args=default_args,
          description='ETL for capstone project',
          schedule_interval='0 0 1 * *',#cron expression of @monthly for every first day of evey month
          max_active_runs= 1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql='capstone_create_tables.sql'
)

stage_i94data = StageToRedshiftOperator(
    task_id='Stage_i94data',
    dag=dag,
    table='staging_i94',
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id= AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key='clean_i94_data/arrival_year={execution_date.year}/arrival_month={execution_date.month}/arrival_day={execution_date.day}',
    region=S3_REGION,
    data_format= "PARQUET"
)

stage_citytempdata = StageToRedshiftOperator(
    task_id='Stage_citytempdata',
    dag=dag,
    table='staging_temp',
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id= AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key='clean_temp_data',
    region=S3_REGION,
    data_format= "PARQUET"
)

stage_portdata = StageToRedshiftOperator(
    task_id='Stage_portdata',
    dag=dag,
    table='staging_port',
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id= AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key='airport-codes_csv.csv',
    region=S3_REGION,
    data_format= "CSV"
)

stage_demogdata = StageToRedshiftOperator(
    task_id='Stage_demogdata',
    dag=dag,
    table='staging_demog',
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id= AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key='us-cities-demographics.csv',
    region=S3_REGION,
    data_format= "CSV"
)

staging_completed = DummyOperator(task_id='Staging_completed',  dag=dag)

load_personali94_dimension_table = LoadDimensionOperator(
    task_id='Load_personali94_dim_table',
    dag=dag,
    table="personali94",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql= SqlQueries.personali94_insert,
    truncate=True
)

load_citytemperatures_dimension_table = LoadDimensionOperator(
    task_id='Load_citytemperatures_dim_table',
    dag=dag,
    table="citytemperatures",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql= SqlQueries.citytemperatures_insert,
    truncate=True
)

load_demographics_dimension_table = LoadDimensionOperator(
    task_id='Load_demographics_dim_table',
    dag=dag,
    table="demographics",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql= SqlQueries.demographics_insert,
    truncate=True
)

load_ports_dimension_table = LoadDimensionOperator(
    task_id='Load_ports_dim_table',
    dag=dag,
    table="ports",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql= SqlQueries.ports_insert,
    truncate=True
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    table="date",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql= SqlQueries.date_insert,
    truncate=True
)

dimension_tables_completed = DummyOperator(task_id='dimension_tables_completed',  dag=dag)

load_facti94_table = LoadFactOperator(
    task_id='Load_facti94_fact_table',
    dag=dag,
    table="facti94",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.facti94,
    truncate=False
)
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    checking_vars= check_query,
    tables=table_names
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables_task 
create_tables_task >> [stage_i94data,stage_citytempdata,stage_portdata,stage_demogdata]>>staging_completed 
staging_completed >> [load_personali94_dimension_table,load_citytemperatures_dimension_table\
                      ,load_demographics_dimension_table,load_ports_dimension_table,load_date_dimension_table ] >>dimension_tables_completed
dimension_tables_completed>>load_facti94_table
load_facti94_table >> run_quality_checks 
run_quality_checks >> end_operator
