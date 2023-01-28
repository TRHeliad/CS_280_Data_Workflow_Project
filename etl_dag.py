import requests, logging
import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models import Variable
from airflow.operators.python import PythonOperator

def get_auth_header():
    bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {bearer_token}"}


def dict_to_str(d):
    return str(d).encode('unicode-escape').decode('utf-8')


def get_twitter_api_data(ti: TaskInstance, **kwargs):
    user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
    tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)

    user_data = {}
    tweet_data = {}

    # Get the user data
    for user_id in user_ids:
        get_user_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id"
        request = requests.get(get_user_url, headers=get_auth_header())
        user_data[user_id] = request.json()

    # Get the tweet data
    for tweet_id in tweet_ids:
        get_tweet_url = f"https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=public_metrics,author_id,text"
        request = requests.get(get_tweet_url, headers=get_auth_header())
        tweet_data[tweet_id] = request.json()

    ti.xcom_push("user_data", user_data)
    ti.xcom_push("tweet_data", tweet_data)


def log_response_data(ti: TaskInstance, **kwargs):
    user_data = ti.xcom_pull(key="user_data", task_ids="get_twitter_api_data_task")
    tweet_data = ti.xcom_pull(key="tweet_data", task_ids="get_twitter_api_data_task")
    logging.info(dict_to_str(user_data))
    logging.info(dict_to_str(tweet_data))


with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(task_id="get_twitter_api_data_task", python_callable=get_twitter_api_data)
    log_response_data_task = PythonOperator(task_id="log_response_data_task", python_callable=log_response_data)

get_twitter_api_data_task >> log_response_data_task