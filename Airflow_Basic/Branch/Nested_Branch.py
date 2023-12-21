from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago


def decide_first_stage(**kwargs):
    if kwargs['ti'].xcom_pull(task_ids='check_data'):
        return 'send_email_task'
    else:
        return 'dummy_task'
    
