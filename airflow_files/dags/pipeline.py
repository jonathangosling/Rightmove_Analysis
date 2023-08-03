from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import main

ODBC_Driver = 'ODBC Driver 17 for SQL Server'
schema = 'test'

def first_ETL():
    main.ETL_to_database(ODBC_Driver = ODBC_Driver, schema = schema)

def second_ETL():
    main.ETL_to_mart(ODBC_Driver = ODBC_Driver, schema = 'dbo')

default_args = {
    'owner': 'jonathangosling',
    'retries': 1,
    'retry_delay': timedelta(hours=2)
}

with DAG(
    default_args = default_args,
    dag_id = 'my_ETL_pipeline',
    start_date = datetime(2023,8,1),
    schedule_interval = '0 13 * * Mon,Fri'
) as dag:
    task1 = PythonOperator(
        task_id = 'ETL_into_database',
        python_callable = first_ETL
    )
    task2 = PythonOperator(
        task_id = 'ETL_into_datamart',
        python_callable = second_ETL
    )

    task1 >> task2
