from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG{
    'Back fill example with partitioned bq table',
    default_args=default_args,
    schedule_interval=None,
}

project = 'your_project'
dataset = 'test_table'
des_table = 'stock_price_des_'
table_date = '{{ ds }}'


start_task = DummyOperator(task_id='start_task', dag=dag)


bq_query_task = BigQueryExecuteQueryOperator(
    task_id='run_bq_query',
    sql='SELECT * FROM your_dataset.your_table WHERE date_field = {{ ds }}',
    use_legacy_sql=False,
    destination_dataset_table=f"{project}.{dataset}.{des_table}{table_date}",
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

final_task = DummyOperator(task_id='final_task', dag=dag)

start_task >> bq_query_task >> final_task