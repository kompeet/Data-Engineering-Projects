from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
import pymongo
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 2),
    'schedule_interval': '@weekly',
    'retries': 0,
    'provide_context': True
}

dag = DAG('load_data_to_mongo', default_args=default_args, catchup=False)


def load_mongodb_airports(ti):
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    sql = """
            SELECT a.name, a.type, a.elevation_ft, a.iso_region, a.gps_code, a.local_code, c.name AS country, c.continent  
            FROM exam.dbo.airports AS a
            INNER JOIN exam.dbo.countries AS c 
            ON a.iso_country = c.iso2_code;
       """
    df = hook.get_pandas_df(sql)
    cluster = MongoClient("mongodb+srv://kompeet:password123@cluster0.wdmaf3t.mongodb.net/?retryWrites=true&w=majority")
    db = cluster["myfirstdb"]
    collection = db["airports"]
    collection.insert_many(df.to_dict('records'))
    return 0


load_mongo_airports = PythonOperator(task_id='load_mongo_airports', python_callable=load_mongodb_airports, dag=dag)

load_mongo_airports
