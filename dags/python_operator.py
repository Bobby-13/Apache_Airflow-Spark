
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def Greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"\n****************************************************************\nHello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!\n****************************************************************\n")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Rachii')
    ti.xcom_push(key='last_name', value='Ravindra')
    

def get_age(ti):
    ti.xcom_push(key='age', value=21)

default_args = {
    'owner': 'myself',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='python_operator_v5',
    default_args=default_args,
    description='Dag Execution using Python Operator',
    start_date=datetime(2024, 5, 1, 2),
    schedule_interval='@daily',
    catchup=True
) as dag:
    
    task1 = PythonOperator(
        task_id = 'Welcome',
        python_callable=Greet,
        op_kwargs={'name':'Boopathi'}
    )
    
    
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
     
     
    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
    )
    
    
    [task2, task3] >> task1