import requests, logging
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import storage

def get_auth_header():
    bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {bearer_token}"}


def dict_to_str(d):
    return str(d).encode('unicode-escape').decode('utf-8')


def get_twitter_api_data(ti: TaskInstance, **kwargs):
    user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
    tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)

    user_data = []
    tweet_data = []

    # Get the user data
    for user_id in user_ids:
        get_user_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id"
        request = requests.get(get_user_url, headers=get_auth_header())
        user_data.append(request.json()["data"])

    # Get the tweet data
    for tweet_id in tweet_ids:
        get_tweet_url = f"https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=public_metrics,author_id,text"
        request = requests.get(get_tweet_url, headers=get_auth_header())
        tweet_data.append(request.json()["data"])

    ti.xcom_push("user_data", user_data)
    ti.xcom_push("tweet_data", tweet_data)


def log_response_data(ti: TaskInstance, **kwargs):
    user_data = ti.xcom_pull(key="user_data", task_ids="get_twitter_api_data_task")
    tweet_data = ti.xcom_pull(key="tweet_data", task_ids="get_twitter_api_data_task")
    logging.info(dict_to_str(user_data))
    logging.info(dict_to_str(tweet_data))


needed_tweet_fields = ["tweet_id", "text", "retweet_count", "reply_count", "like_count", "quote_count", "impression_count"]
needed_user_fields = ["user_id", "username", "name", "followers_count", "following_count", "tweet_count", "listed_count"]
def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
    user_data = ti.xcom_pull(key="user_data", task_ids="get_twitter_api_data_task")
    tweet_data = ti.xcom_pull(key="tweet_data", task_ids="get_twitter_api_data_task")

    # Move "public_metrics" values to parent and rename id
    for data in user_data:
        for key, metric in data["public_metrics"].items():
            data[key] = metric
        data["public_metrics"] = None

    for data in tweet_data:
        for key, metric in data["public_metrics"].items():
            data[key] = metric
        data["public_metrics"] = None

    user_df = pd.DataFrame(data=user_data)
    tweet_df = pd.DataFrame(data=tweet_data)

    # Rename id cols
    user_df = user_df.rename(columns={"id": "user_id"})
    tweet_df = tweet_df.rename(columns={"id": "tweet_id"})

    # Remove unnecessary columns
    user_cols = list(user_df.columns)
    tweet_cols = list(tweet_df.columns)
    remove_user_cols = [x for x in user_cols if not x in needed_user_fields]
    logging.info(remove_user_cols)
    remove_tweet_cols = [x for x in tweet_cols if not x in needed_tweet_fields]
    logging.info(remove_tweet_cols)
    user_df = user_df.drop(columns=remove_user_cols)
    tweet_df = tweet_df.drop(columns=remove_tweet_cols)

    client = storage.Client()
    bucket = client.get_bucket("b-m-apache-airflow-cs280")
    bucket.blob("data/user_data.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob("data/tweet_data.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")


with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(
        task_id="get_twitter_api_data_task",
        python_callable=get_twitter_api_data,
        provide_context=True
    )
    log_response_data_task = PythonOperator(
        task_id="log_response_data_task",
        python_callable=log_response_data
    )
    transform_twitter_api_data_task = PythonOperator(
        task_id="transform_twitter_api_data_task",
        python_callable=transform_twitter_api_data_func,
        provide_context=True
    )

get_twitter_api_data_task >> log_response_data_task >> transform_twitter_api_data_task