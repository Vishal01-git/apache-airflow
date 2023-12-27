from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 24),
    'catchup':False,
    'retries': 0,
}

dag = DAG(
    'Back_Fill',
    default_args=default_args,
    schedule_interval='@daily',
    
)

project = 'friendly-plane-294914'
dataset = 'test_table'
des_table = 'stock_price_des_'
table_date = str('{{ ds }}').replace("-", "")



start_task = DummyOperator(task_id='start_task', dag=dag)


bq_query_task = BigQueryExecuteQueryOperator(
    task_id='run_bq_query',
    sql="""SELECT * FROM `friendly-plane-294914.test_table.stock_price_data` where Date = CAST('{{ ds }}' AS DATE) """,
    use_legacy_sql=False,
    destination_dataset_table=f"{project}.{dataset}.{des_table}{table_date}",
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    dag=dag,
)

final_task = DummyOperator(task_id='final_task', dag=dag)

start_task >> bq_query_task >> final_task