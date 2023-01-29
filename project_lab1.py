import requests, logging
import pendulum
import pandas as pd
from databox import Client
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import storage
from io import StringIO


def get_auth_header():
    bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {bearer_token}"}


def get_databox_token():
    return Variable.get("DATABOX_TOKEN")


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


def update_databox():#ti: TaskInstance, **kwargs):
    # Get current time and format
    utc_time = pendulum.now('UTC')
    formatted_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')

    # Get the data from the bucket
    client = storage.Client()
    bucket = client.get_bucket("b-m-apache-airflow-cs280")

    # Get the databox client
    databox_client = Client(get_databox_token())
    
    # First we need to convert the strings we download into a buffer
    # This is necessary since pandas.read_csv requires a path or a buffer
    user_data_csv_buffer = StringIO(bucket.blob("data/user_data.csv").download_as_text())
    tweet_data_csv_buffer = StringIO(bucket.blob("data/tweet_data.csv").download_as_text())

    # Now we can create the dataframes
    user_df = pd.read_csv(user_data_csv_buffer)
    tweet_df = pd.read_csv(tweet_data_csv_buffer)

    # I am going to push the metrics for Linus Tech Tips (user_id is 403614288)
    target_user_metrics = ["followers_count", "following_count", "tweet_count", "listed_count"]
    ## This is a series of the linus row
    linus_row = user_df.loc[user_df['user_id']==403614288].iloc[0]

    ## Push the user metrics
    username = linus_row["username"]
    for metric in target_user_metrics:
        databox_client.push(f"{username}_{metric}", int(linus_row[metric]), date=formatted_time)

    # I am going to use the fishing tweet from President Nelson
    target_tweet_metrics = ["reply_count", "like_count", "impression_count", "retweet_count"]
    ## This is a series of the tweet's row
    tweet_row = tweet_df.loc[tweet_df['tweet_id']==1609550010969980928].iloc[0]

    ## Push the tweet metrics
    for metric in target_tweet_metrics:
        databox_client.push(metric, int(tweet_row[metric]), date=formatted_time)

update_databox()

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(
        task_id="get_twitter_api_data_task",
        python_callable=get_twitter_api_data
    )
    log_response_data_task = PythonOperator(
        task_id="log_response_data_task",
        python_callable=log_response_data
    )
    transform_twitter_api_data_task = PythonOperator(
        task_id="transform_twitter_api_data_task",
        python_callable=transform_twitter_api_data_func
    )
    update_databox_task = PythonOperator(
        task_id="update_databox_task",
        python_callable=update_databox
    )

get_twitter_api_data_task >> log_response_data_task >> transform_twitter_api_data_task >> update_databox_task