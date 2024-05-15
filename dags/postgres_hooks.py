import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore


default_args = {
    'owner': 'myself',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def create_orders_table():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        conn = hook.get_conn()       
        cursor = conn.cursor()
        create_table_query = "CREATE TABLE IF NOT EXISTS orders ( order_id VARCHAR(50),date DATE,product_name VARCHAR(100),quantity INTEGER,PRIMARY KEY (order_id))"
        cursor.execute(create_table_query)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Orders table created successfully")
    except Exception as e:
        print("Error:", e)
        
            
def push_data_to_postgres():
    try:
        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        conn = hook.get_conn()       
        cursor = conn.cursor()
        
        print("hello hi")
        with open('dags/Orders.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)  
            for row in reader:
                order_id, date, product_id, quantity = row
                cursor.execute("INSERT INTO orders (order_id, date, product_name, quantity) VALUES (%s, %s, %s, %s)", (order_id, date, product_id, quantity))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully pushed to PostgreSQL")
    except Exception as e:
        print("Error:", e)

with DAG(
    dag_id="dag_with_postgres_hooks_v012",
    default_args=default_args,
    start_date=datetime(2024, 5, 8),
    schedule_interval='@daily'
) as dag:
    
    task2 = PythonOperator(
        task_id = "create_orders_table",
        python_callable=create_orders_table
    )
    
    task1 = PythonOperator(
        task_id="push_csv_file_data_to_postgres",
        python_callable=push_data_to_postgres
    )
    
    task2 >> task1