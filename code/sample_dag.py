from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 1, 1),
    'end_date': datetime(2022, 12, 31),
    'catchup': False,
    'tags': ['example', 'tutorial'],
    'max_active_runs': 1
}

dag = DAG(
    dag_id = "demo_dags_airflow",
    default_args = default_args,
    description = "Learning Airflow DAGs",
    schedule_interval = timedelta(days=1,hours=1,minutes=0),
    start_date = datetime(2024, 1, 1),
    catchup = False,
    max_active_runs= 1
)

def get_weather_infor():
    print("Extracting data from an weather API")
    return {
        "date": "2023-01-01",
        "location": "NYC",
        "weather": {
        "temp": 33,
        "conditions": "Light snow and wind"
        }
    }

def greet_the_audience():
    print("Hello weather enthuasits")

def give_weather_infor():
    print(get_weather_infor())

def exit_weather_meeting():
    print("Bye bye weather people")

task_greet = PythonOperator(task_id = "task_greet", python_callable=greet_the_audience, dag=dag)
task_address = PythonOperator(task_id = "task_address", python_callable=give_weather_infor, dag=dag)
task_exit = PythonOperator(task_id = "task_exit", python_callable=exit_weather_meeting, dag=dag)

task_greet >> task_address >> task_exit
