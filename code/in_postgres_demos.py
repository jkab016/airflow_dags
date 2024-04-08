from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import csv

def write_to_file(**kwargs):
    # Write data to a file
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='extract_data')
    with open("test.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 5),
    'retries': 1
}

dag = DAG('postgres_to_file', default_args=default_args, schedule_interval=None)

extract_data_task = PostgresOperator(
    task_id='extract_data',
    postgres_conn_id = 'local_postgres',
    sql='SELECT * FROM dev.account',
    dag = dag
)

write_to_file_task = PythonOperator(
    task_id='write_to_file',
    python_callable=write_to_file,
    op_kwargs={'rows': '{{ task_instance.xcom_pull(task_ids="extract_data") }}'},
    dag = dag
)

extract_data_task >> write_to_file_task
