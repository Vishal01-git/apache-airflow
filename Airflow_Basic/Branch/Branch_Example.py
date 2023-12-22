from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago


def notification_email(**kwargs):
    if kwargs['ti'].xcom_pull(task_ids='check_data'):
        return 'send_email_task'
    else:
        return 'dummy_task'

def always_true():
    return True
    

        
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'branch_example',
    default_args=default_args,
    schedule_interval=None,
)

start_task = DummyOperator(task_id='start_task', dag=dag)


check_data = PythonOperator(
    task_id='check_data',
    python_callable=always_true,
    dag=dag,
)

branch_task = PythonOperator(
    task_id='branch_task',
    python_callable=notification_email,
    provide_context=True,
    dag=dag,
)

send_email_task = EmailOperator(
    task_id='send_email_task',
    to ='example@example.com',
    subject='Airflow Alert',
    html_content=""" <h3>Email Test</h3> """,
    dag=dag,
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

start_task >> check_data >> branch_task >> [send_email_task, dummy_task]


