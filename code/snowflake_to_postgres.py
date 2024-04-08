from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

# Define your default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define the DAG
dag = DAG(
    'snowflake_to_postgres',
    default_args=default_args,
    description='A DAG to move data from Snowflake to PostgreSQL',
    schedule_interval=None
)

# Define the Snowflake SQL command to extract data
snowflake_sql = """
SELECT * FROM test_raw.test.students;
"""

# Define the Python function to transfer data
def transfer_data(**context):
    # Fetch the Snowflake data
    snowflake_hook = context['ti'].xcom_pull(task_ids='extract_from_snowflake')
    df = pd.DataFrame(snowflake_hook)

    # Convert DataFrame to a list of tuples
    records = list(df.itertuples(index=False, name=None))

    # Insert data into PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='local_postgres')
    postgres_hook.insert_rows(table='dev.students', rows=records, target_fields=df.columns.tolist(), create_table=True)

# Define the Snowflake task to extract data
extract_from_snowflake = SnowflakeOperator(
    task_id='extract_from_snowflake',
    sql=snowflake_sql,
    snowflake_conn_id='snowflake_eon',
    dag=dag,
)

# Define the Python task to transfer data
transfer_data_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    provide_context=True,
    dag=dag,
)

prep_tables = PostgresOperator(
    task_id='prep_tables',
    postgres_conn_id = 'local_postgres',
    sql='truncate table dev.students',
    dag = dag
)

# Set the task dependencies
prep_tables >> extract_from_snowflake >> transfer_data_task
