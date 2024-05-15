from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

def helloworld():
    print(f"\n****************************************************************\nHello World ! .. . . . . . . . . \n****************************************************************\n")

default_args = {
    'owner':'myself',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='scheduler_crontab_v5',
    default_args=default_args,
    description='Crontab Excution in scheduler',
    start_date=datetime(2024, 5, 1),
    schedule_interval='0 3 * * *',
    # catchup=True
) as dag:
    
    task1 = PythonOperator(
        task_id = 'hello_world',
        python_callable= helloworld
    )
    
    task1
    