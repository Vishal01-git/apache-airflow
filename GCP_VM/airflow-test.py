from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Define a function to be used in the PythonOperator
def my_python_function():
    print("Hello from the PythonOperator!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=my_python_function,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set the task dependencies
start_task >> python_task >> end_task
