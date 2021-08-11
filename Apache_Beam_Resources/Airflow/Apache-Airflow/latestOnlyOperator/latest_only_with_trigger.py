from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

with DAG(dag_id='latest_only_example', schedule_interval=timedelta(hours=6), start_date=datetime(2020, 1, 24), catchup=True) as dag:

    t1 = LatestOnlyOperator(task_id = 'latest_only')

    t2 = DummyOperator(task_id='task2')

    t3 = DummyOperator(task_id='task3')

    t4 = DummyOperator(task_id='task4')

    t5 = DummyOperator(task_id='task5')

    t1 >> [t2, t4, t5]
