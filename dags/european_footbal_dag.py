import datetime
import json
import os
import pickle 

import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from airflow.utils.dates import days_ago
from airflow.models.variable import Variable

from helpers.sqlite_to_avro import sqlite_to_avro

base_path = os.path.dirname(os.path.abspath(__file__))

#AIRFLOW VARIABLE
HOME_PATH = Variable.get('HOME_PATH')  
BUCKET_NAME = Variable.get('GCS_BUCKET_NAME')
STAGING_DATASET = Variable.get('STAGING_DATASET')
PRODUCTION_DATASET = Variable.get("PRODUCTION_DATASET")
PROJECT_NAME = Variable.get('PROJECT_NAME')
AIRFLOW_CONN_GCP_CONN = Variable.get("AIRFLOW_CONN_GCP_CONN")


#file path
data_source_path = os.path.join(HOME_PATH,'data')
data_result_path = os.path.join(HOME_PATH,'data_result')

# avro_schema_folder = os.path.join(data_source_path,'avro_schema')

tables = ["Country","Match","Player_Attributes","Team_Attributes","League","Player","Team"]
# tables = ["Player_Attributes"]
sqlite_db_path = os.path.join(data_source_path,*['sqlite_data','database.sqlite'])

default_args = {
    'owner': 'kurniawan',
    'depends_on_past': False,
    'email': ['data@engineer.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    
}
dag = DAG(
    'sqlite_to_bq',
    default_args=default_args,
    description="A simple DAG to move SQLite data into Bigquery",
    schedule_interval="0 4 * * * ",
    start_date=days_ago(1),
    tags = ['avro','sqlite','bigquery']
)
start = DummyOperator(task_id='start',dag=dag)
end = DummyOperator(task_id='end',dag=dag)

for table in tables:
    table = table.lower()
    file_name = f"sqlite_{table}.avro"
    gcs_object_path = f"sqlite/{file_name}"
    
    destination_path = os.path.join(data_result_path,file_name)
    avro_schema_path = os.path.join(data_source_path,*['avro_schema',f"{table}.json"])
    query_path = os.path.join(data_source_path,*['sqlite_data',f"{table}.sql"])

    convert_file = PythonOperator(
        task_id = f'convert_{table}_to_avro',
        python_callable=sqlite_to_avro,
        op_kwargs={
            "sqlite_db":sqlite_db_path,
            "query_path":query_path,
            "avro_schema_path":avro_schema_path,
            "destination_path":destination_path
        },
        dag=dag
    )
    upload_gcs = LocalFilesystemToGCSOperator(
        task_id=f'store_{table}_to_gcs',
        gcp_conn_id=AIRFLOW_CONN_GCP_CONN,
        src=destination_path,
        dst=gcs_object_path,
        bucket=BUCKET_NAME,
        dag=dag
    )
    
    gcs_bq = GCSToBigQueryOperator(
        task_id=f'load_{table}_to_bq',
        bigquery_conn_id=AIRFLOW_CONN_GCP_CONN,
        bucket=BUCKET_NAME,
        source_objects=[gcs_object_path],
        destination_project_dataset_table=f"{STAGING_DATASET}.sqlite_{table.lower()}",
        source_format='AVRO',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        src_fmt_configs={'useAvroLogicalTypes': True},
        dag=dag
    )


    start >>convert_file >> upload_gcs>>gcs_bq>>end

    