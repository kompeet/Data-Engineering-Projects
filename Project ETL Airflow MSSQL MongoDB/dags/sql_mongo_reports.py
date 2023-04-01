from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 2),
    'schedule_interval': '@weekly',
    'retries': 0,
    'provide_context': True
}

dag = DAG('sql_and_mongo_reports', default_args=default_args, catchup=False)


def sql_aggregation_01():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT TOP 10 WITH TIES name, elevation_ft
        FROM exam.dbo.airports
        ORDER BY elevation_ft DESC
        """
    return hook.get_records(sql)


def sql_aggregation_02():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT a.name AS 'Airport', c.name AS 'Country', c.continent AS 'Continent'
        FROM exam.dbo.airports a
        LEFT JOIN exam.dbo.countries c 
        ON a.iso_country = c.iso2_code
        WHERE c.continent = 'Africa'
        """
    return hook.get_records(sql)


def sql_aggregation_03():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT name, local_code
        FROM exam.dbo.airports
        WHERE local_code LIKE '02%'
        """
    return hook.get_records(sql)


def sql_aggregation_04():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
       SELECT MAX(a.elevation_ft), c.continent
       FROM exam.dbo.airports a
       LEFT JOIN exam.dbo.countries c 
       ON a.iso_country = c.iso2_code
       GROUP BY c.continent
       """
    return hook.get_records(sql)


def sql_aggregation_05():
    hook = MsSqlHook(mssql_conn_id='mssql_conn')
    sql = """
        SELECT COUNT(*)
        FROM exam.dbo.airports
        WHERE type LIKE 'small_airport'
        """
    return hook.get_records(sql)


def mongo_report_01():
    client = MongoClient("mongodb+srv://kompeet:password123@cluster0.wdmaf3t.mongodb.net/?retryWrites=true&w=majority")
    db = client["myfirstdb"]
    collection = db["airports"]
    result = collection.find({"continent": "Europe"}, {"name": 1, "continent": 1, "_id": 0}).sort("elevation_ft", -1)
    return list(result)


def mongo_report_02():
    client = MongoClient("mongodb+srv://kompeet:password123@cluster0.wdmaf3t.mongodb.net/?retryWrites=true&w=majority")
    db = client["myfirstdb"]
    collection = db["airports"]
    result = collection.count_documents({"type": "heliport"})
    return result


sql_aggregation_01 = PythonOperator(task_id='sql_aggregation_01', python_callable=sql_aggregation_01, dag=dag)
sql_aggregation_02 = PythonOperator(task_id='sql_aggregation_02', python_callable=sql_aggregation_02, dag=dag)
sql_aggregation_03 = PythonOperator(task_id='sql_aggregation_03', python_callable=sql_aggregation_03, dag=dag)
sql_aggregation_04 = PythonOperator(task_id='sql_aggregation_04', python_callable=sql_aggregation_04, dag=dag)
sql_aggregation_05 = PythonOperator(task_id='sql_aggregation_05', python_callable=sql_aggregation_05, dag=dag)
mongo_report_01 = PythonOperator(task_id='mongo_report_01', python_callable=mongo_report_01, dag=dag)
mongo_report_02 = PythonOperator(task_id='mongo_report_02', python_callable=mongo_report_02, dag=dag)

[sql_aggregation_01, sql_aggregation_02, sql_aggregation_03, sql_aggregation_04, sql_aggregation_04, mongo_report_01,
 mongo_report_02]
