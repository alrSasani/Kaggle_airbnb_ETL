
from datetime import datetime,timedelta
import os
from pathlib import Path
import json

from extract import extract
from transform import transform
from load import load

from airflow import DAG

# Database information from Kaggle
dataset_name = 'swsw1717/seatle-airbnb-open-data-sql-project'
csv_files = ['listings.csv','calendar.csv','reviews.csv']

downloads_path = Path(os.getcwd(),'dags/data_donld')
transform_path = Path(os.getcwd(),'dags/data_trnsfrm')

# Postgresql Database in user information:
with open('config/db_config.json','r') as info:
    db_params = json.load(info)["DB_info"]

default_args = {
    'owner': 'Alireza',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
    

with DAG(dag_id='Kaggle_airbnb_ETL',
         description='An ETL to load data from kaggle using kaggle api and loading it to postgresql database',
         default_args=default_args,
         schedule=None,
         start_date=datetime(year=2024, month=8, day=15),
         catchup=False,
         schedule_interval=None,
         max_active_runs=1,
         tags=['Kaggle_airbnb_ETL']) as dag:
    
    (extract(dataset_name, downloads_path) >> 
     transform(csv_files,downloads_path,transform_path) >> 
     load(csv_files,transform_path, db_params))

    
    
