from airflow import DAG
import logging as log
import pendulum
from airflow.operators.smooth import SmoothOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="Smooth_DAG",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 9, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    smooth_task = SmoothOperator()
    end_task = DummyOperator(task_id="end_task")


start_task >> smooth_task >> end_task