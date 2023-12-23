from airflow.contrib.operators.bigquery_operator import BigQueryOperator

bq_query_task = BigQueryOperator(
    task_id='run_bq_query',
    sql='SELECT * FROM your_dataset.your_table WHERE date_field = {{ ds }}',
    use_legacy_sql=False,
    destination_dataset_table='your_dataset.destination_table',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    dag=your_dag,
)