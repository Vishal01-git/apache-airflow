from airflow import DAG
from google.cloud import bigquery
import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'Table_Update_Check',
    default_args=default_args,
    schedule_interval=None,
    tags=['example'],
)

def check_table_update():
    client = bigquery.Client()
    query = """
        SELECT  DATE(TIMESTAMP_MILLIS(last_modified_time)) AS DatePeriod FROM `bigquery-public-data.austin_311.__TABLES__` where table_id='311_service_requests'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        if str(row.DatePeriod)==str(datetime.datetime.now().strftime("%Y-%m-%d")):
            return 'send_email'
        else:
            return 'do_nothing'
        
    start_tast = DummyOperator(task_id='start_task', dag=dag)
    
    check_table_update = BranchPythonOperator(
        task_id='check_table_update',
        python_callable=check_table_update,
        dag=dag,
    )
    
    email_task = EmailOperator(
        task_id='send_email',
        to='e@gmail.com',
        subject='Table Updated',
        html_content=""" <h3>Table Updated</h3> """,
        dag=dag,
    )
    
    do_nothing = DummyOperator(task_id='do_nothing', dag=dag)
    
    end_task = DummyOperator(task_id='end_task', dag=dag)
    
    
    start_tast >> check_table_update >> [email_task, do_nothing] >> end_task
    
    