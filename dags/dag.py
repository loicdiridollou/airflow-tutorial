from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint


def t1():
    return randint(1, 10)


def _choose_best_model(ti):
    best_accuracy = max(ti.xcom_pull(task_ids=['training_a', 'training_b', 'training_c']))
    if best_accuracy > 5:
        return 'accurate1'
    else:
        return 'inaccurate1'


with DAG('dag', start_date=datetime(2021, 10, 14), schedule_interval='@daily', catchup=False) as dag:
    training_model_a = PythonOperator(
        task_id='training_a',
        python_callable=t1
    )
    training_model_b = PythonOperator(
        task_id='training_b',
        python_callable=t1
    )
    training_model_c = PythonOperator(
        task_id='training_c',
        python_callable=t1
    )
    choose_best = BranchPythonOperator(
        task_id='choose_best',
        python_callable=_choose_best_model,

    )

    accurate = BashOperator(
        task_id='accurate1',
        bash_command="echo 'accurate'"
    )
    inaccurate = BashOperator(
        task_id='inaccurate1',
        bash_command="echo 'inaccurate'"
    )

    [training_model_a, training_model_b, training_model_c] >> choose_best >> [accurate, inaccurate]