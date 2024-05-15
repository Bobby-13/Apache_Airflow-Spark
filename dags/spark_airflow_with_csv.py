import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from airflow.operators.bash import BashOperator
dag = DAG(
    dag_id = "sparking_flow_with_csv_53",
    default_args = {
        "owner": "airflow",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

def install_pandas():
    import subprocess
    subprocess.call(["pip", "install", "pandas"])

install_pandas_operator = PythonOperator(
    task_id='install_pandas_task',
    python_callable=install_pandas,
    dag=dag
)

# python_job_1 = SparkSubmitOperator(
#     task_id="word_count",
#     conn_id="spark-conn",
#     application="jobs/wordcount.py",
#     dag=dag
# )


python_job_2 = SparkSubmitOperator(
    task_id="read_csv_file",
    conn_id="spark-conn",
    application="jobs/spark_dataframe.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> install_pandas_operator >> python_job_2 >> end