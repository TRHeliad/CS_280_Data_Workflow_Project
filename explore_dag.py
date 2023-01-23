from airflow import DAG
import logging as log
import pendulum
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="Email_DAG",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 1, 20, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    email_task = EmailOperator(
        task_id="email_task", 
        to="bmadsenonpc@gmail.com",
        subject="Someone ran your task",
        html_content="This email is being sent from your airflow server"
    )
    end_task = DummyOperator(task_id="end_task")


start_task >> email_task >> end_task