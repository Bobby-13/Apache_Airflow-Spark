import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
dag = DAG(
    dag_id = "spark_airflow_postgres_25",
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


spark_submit_command = "spark-submit --driver-class-path ${SPARK_HOME}/opt/airflow/jobs/lib/postgresql-42.7.3.jar ${SPARK_HOME}/opt/airflow/jobs/postgres_spark.py"
bash_job = BashOperator(
    task_id='spark_job',
    bash_command=spark_submit_command,
    dag=dag,
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)


start >> bash_job >> end