import airflow    
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='example_branch_datetime',
    default_args=args,
    schedule_interval='@daily',
    tags=['example']
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

branch_task = BranchDateTimeOperator(
    task_id='branch_task',
    follow_task_ids_if_true='email_bob',
    follow_task_ids_if_false='email_alice',
    execution_date="{{ execution_date }}",
    trigger_rule='all_done',
    dag=dag,
)

email_bob = DummyOperator(
    task_id='email_bob',
    dag=dag,
)

email_alice = DummyOperator(
    task_id='email_alice',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED, # Ensure end is executed even if email_bob or email_alice were skipped
    dag=dag,
)

start >> branch_task >> [email_bob, email_alice] >> end


