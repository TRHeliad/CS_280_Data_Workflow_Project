from airflow import DAG
import logging as log
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="Email_DAG",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 1, 20, tz="US/Pacific"),
    catchup=False,
) as dag:
    trigger_task = TriggerDagRunOperator(
        task_id="trigger_task",
        trigger_dag_id="My_First_CS_280_DAG",
        reset_dag_run=True
    )