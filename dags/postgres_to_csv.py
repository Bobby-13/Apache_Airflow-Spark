import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

default_args = {
    'owner': 'myself',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def postgres_to_file():
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date >= '20220501'")
    
    with open("dags/get_orders.csv", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)    
    cursor.close()
    conn.close()
    logging.info("Saved orders data in text file") 
    
with DAG(
    dag_id="dag_with_postgres_to_file_3",
    default_args=default_args,
    start_date=datetime(2024, 5, 8),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_file",
        python_callable=postgres_to_file
    )
    
    task1