import pandas
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
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

dag = DAG('airports_json_etl', default_args=default_args, catchup=False)


def get_data_from_api():
    url = "https://pkgstore.datahub.io/core/airport-codes/airport-codes_json/data/9ca22195b4c64a562a0a8be8d133e700/airport-codes_json.json"
    read = requests.get(url)
    data = read.json()
    df = pandas.json_normalize(data)
    df2 = pandas.DataFrame(df)
    df.drop(columns=["continent"], inplace=True)
    df3 = df2[["ident", "type", "name", "elevation_ft", "iso_country", "iso_region", "municipality", "gps_code",
               "iata_code", "local_code", "coordinates"]]
    return df3.values.tolist()


def transform(ti):
    data = ti.xcom_pull(task_ids=['extract'])[0]
    df = pandas.DataFrame(data,
                          columns=["ident", "type", "name", "elevation_ft", "iso_country", "iso_region", "municipality",
                                   "gps_code", "iata_code", "local_code", "coordinates"])
    df.fillna("0", inplace=True)
    df.dropna(subset=["gps_code"], inplace=True)
    return df.values.tolist()


def load_mssql(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    hook = MsSqlHook(mssql_conn_id="mssql_conn")
    hook.insert_rows("exam.dbo.airports", data)
    return 0


# def load_mongodb(ti):
#     hook = MsSqlHook(mssql_conn_id="mssql_conn")
#     sql = """
#            SELECT name, type, elevation_ft, iso_region, gps_code, local_code
#            FROM exam.dbo.airports
#        """
#     df = hook.get_pandas_df(sql)
#     cluster = MongoClient("mongodb+srv://kompeet:password123@cluster0.wdmaf3t.mongodb.net/?retryWrites=true&w=majority")
#     db = cluster["myfirstdb"]
#     collection = db["airports"]
#     collection.insert_many(df.to_dict('records'))
#     return 0

extract = PythonOperator(task_id='extract', python_callable=get_data_from_api, dag=dag)
transform = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
load_mssql = PythonOperator(task_id='load_mssql', python_callable=load_mssql, dag=dag)
# load_mongo = PythonOperator(task_id='load_mongo', python_callable=load_mongodb, dag=dag)

extract >> transform >> load_mssql  # >> load_mongo
