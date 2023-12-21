from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def notification_email(**kwargs):
    if kwargs['ti'].xcom_pull(task_ids='check_data'):
        return 'send_email_task'
    else:
        return 'dummy_task'
    

        


