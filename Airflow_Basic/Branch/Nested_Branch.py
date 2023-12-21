from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

def decide_outer_branch(**kwargs):
    # Your outer branching logic goes here
    if some_condition:
        return 'branch_outer_true'
    else:
        return 'branch_outer_false'

def decide_inner_branch(**kwargs):
    # Your inner branching logic goes here
    if some_other_condition:
        return 'branch_inner_true'
    else:
        return 'branch_inner_false'

def outer_true_task(**kwargs):
    # Your logic for the outer true branch
    print("Executing outer true branch")

def outer_false_task(**kwargs):
    # Your logic for the outer false branch
    print("Executing outer false branch")

def inner_true_task(**kwargs):
    # Your logic for the inner true branch
    print("Executing inner true branch")

def inner_false_task(**kwargs):
    # Your logic for the inner false branch
    print("Executing inner false branch")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'nested_branching_example',
    default_args=default_args,
    schedule_interval=None,
)

start_task = DummyOperator(task_id='start_task', dag=dag)

# Outer branching
branch_outer = BranchPythonOperator(
    task_id='branch_outer',
    python_callable=decide_outer_branch,
    provide_context=True,
    dag=dag,
)

branch_outer_true = DummyOperator(task_id='branch_outer_true', dag=dag)
branch_outer_false = DummyOperator(task_id='branch_outer_false', dag=dag)

# Inner branching (inside the outer true branch)
branch_inner = BranchPythonOperator(
    task_id='branch_inner',
    python_callable=decide_inner_branch,
    provide_context=True,
    dag=dag,
)

branch_inner_true = PythonOperator(
    task_id='branch_inner_true',
    python_callable=inner_true_task,
    provide_context=True,
    dag=dag,
)

branch_inner_false = PythonOperator(
    task_id='branch_inner_false',
    python_callable=inner_false_task,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Define the execution flow
start_task >> branch_outer
branch_outer_true >> branch_inner
branch_outer_false >> end_task
branch_inner_true >> end_task
branch_inner_false >> end_task
