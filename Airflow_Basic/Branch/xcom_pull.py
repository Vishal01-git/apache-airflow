from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

def push_function(**kwargs):
    emp = ['Andrew', 'Joel', 'Mark']
    return emp

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

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    dag=dag)

def pull_function(**kwargs):
    emp_name = kwargs['ti'].xcom_pull(task_ids='push_task')
    print(emp_name)

pull_task = BranchPythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    dag=dag)

dummy_task_final = DummyOperator(task_id='dummy_task_final', trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

start_task >> push_task >> pull_task >> dummy_task_final

