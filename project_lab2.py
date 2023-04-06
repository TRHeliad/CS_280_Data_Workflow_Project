import requests, logging
import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pandas as pd
from io import StringIO

from models.config import Session
from models.users import User
from models.user_timeseries import User_Timeseries
from models.tweets import Tweet
from models.tweet_timeseries import Tweet_Timeseries


def get_auth_header():
    bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f"Bearer {bearer_token}"}


def get_last_5_tweets(user_id):
    request_url = f"https://api.twitter.com/2/users/{user_id}/tweets?max_results=5&tweet.fields=public_metrics,created_at"
    request = requests.get(request_url, headers=get_auth_header())
    parsed_request = request.json()["data"]

    for tweet_dict in parsed_request:
        # Add user_id to tweet data
        tweet_dict["user_id"] = user_id
        # Rename id
        tweet_dict["tweet_id"] = tweet_dict["id"]
        tweet_dict["id"] = None

    return parsed_request


def get_user_stats(user_id):
    request_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics"
    request = requests.get(request_url, headers=get_auth_header())
    return request.json()["data"]["public_metrics"]

def get_tweet_data(tweet_id):
    request_url = f"https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=public_metrics"
    request = requests.get(request_url, headers=get_auth_header())
    return request.json()["data"]

# Converts the rows to a list of dictionaries containing only column data
def format_rows(list):
    new_list = []
    for row in list:
        d = {}
        new_list.append(d)
        for column in row.__table__.columns:
            d[column.name] = str(getattr(row, column.name))

    return new_list


def load_data(ti: TaskInstance, **kwargs):
    session = Session()

    # Pull user and tweet data from the database
    allUsersRows = format_rows(session.query(User).all())
    allTweetsRows = format_rows(session.query(Tweet).all())

    # Send the data through xcom for use in the call_api task
    ti.xcom_push("allUsersRows", allUsersRows)
    ti.xcom_push("allTweetsRows", allTweetsRows)
    ti.xcom_push("originalTweetCount", len(allTweetsRows))

    session.close()


def call_api(ti: TaskInstance, **kwargs):
    allUsersRows = ti.xcom_pull(key="allUsersRows", task_ids="load_data_task")
    allTweetsRows = ti.xcom_pull(key="allTweetsRows", task_ids="load_data_task")

    allUserStats = {}
    allRecentTweets = []

    # Get user data
    for user in allUsersRows:
        # Get the most recent 5 tweets
        allRecentTweets.extend(get_last_5_tweets(user["user_id"]))
        # Gets the updated user statistics
        allUserStats[user["id"]] = get_user_stats(user["user_id"])

    # Get tweet data
    updatedTweetData = {}
    for tweet in allTweetsRows:
        updatedTweetData[tweet["id"]] = get_tweet_data(tweet["tweet_id"])

    # Send the data through xcom
    ti.xcom_push("allUserStats", allUserStats)
    ti.xcom_push("allRecentTweets", allRecentTweets)
    ti.xcom_push("updatedTweetData", updatedTweetData)


def transform_data(ti: TaskInstance, **kwargs):
    # Get all the collected information
    allUsersRows = ti.xcom_pull(key="allUsersRows", task_ids="load_data_task")
    allTweetsRows = ti.xcom_pull(key="allTweetsRows", task_ids="load_data_task")
    allUserStats = ti.xcom_pull(key="allUserStats", task_ids="call_api_task")
    allRecentTweets = ti.xcom_pull(key="allRecentTweets", task_ids="call_api_task")
    updatedTweetData = ti.xcom_pull(key="updatedTweetData", task_ids="call_api_task")

    # Take all the previous data and combine it into two dataframes: users and tweets

    ## Merge users row data and user stats
    combinedUserData = []
    for user_dict in allUsersRows:
        id = user_dict["id"]
        combinedUserData.append(user_dict)
        user_dict["followers_count"] = allUserStats[id]["followers_count"]
        user_dict["following_count"] = allUserStats[id]["following_count"]
        user_dict["tweet_count"] = allUserStats[id]["tweet_count"]
        user_dict["listed_count"] = allUserStats[id]["listed_count"]
    
    usersDF = pd.DataFrame(combinedUserData)

    ## Merge tweet row data and tweet stats
    newTweetFilter = [True] * len(allRecentTweets)
    combinedTweetData = []
    for tweet_dict in allTweetsRows:
        id = tweet_dict["id"]
        public_metrics = updatedTweetData[id]["public_metrics"]
        combinedTweetData.append(tweet_dict)
        tweet_dict["retweet_count"] = public_metrics["retweet_count"]
        tweet_dict["like_count"] = public_metrics["like_count"]
        tweet_dict["text"] = updatedTweetData[id]["text"]

        # Mark tweets that are already in the database
        for i, recent_tweet in enumerate(allRecentTweets):
            if recent_tweet["tweet_id"] == tweet_dict["tweet_id"]:
                newTweetFilter[i] = False

    # Add new tweets found in recents
    for i, recent_tweet in enumerate(allRecentTweets):
        if newTweetFilter[i]:
            new_tweet_dict = {
                "id": len(combinedTweetData)+1,
                "tweet_id": recent_tweet["tweet_id"],
                "user_id": recent_tweet["user_id"],
                "text": recent_tweet["text"],
                "created_at": pendulum.parse(recent_tweet["created_at"]).date(),
                "retweet_count": recent_tweet["public_metrics"]["retweet_count"],
                "like_count": recent_tweet["public_metrics"]["like_count"]
            }
            combinedTweetData.append(new_tweet_dict)

    tweetsDF = pd.DataFrame(combinedTweetData)

    # Save the data to the bucket
    bucket_client = storage.Client()
    bucket = bucket_client.get_bucket("b-m-apache-airflow-cs280")
    bucket.blob("data/user_data.csv").upload_from_string(usersDF.to_csv(index=False), "text/csv")
    bucket.blob("data/tweet_data.csv").upload_from_string(tweetsDF.to_csv(index=False), "text/csv")


def write_data(ti: TaskInstance, **kwargs):
    # Use the dataframes and create new timeseries entries using the current data for each user and tweet
    # Then save any new tweets to the database

    # Get the data from the bucket
    bucket_client = storage.Client()
    bucket = bucket_client.get_bucket("b-m-apache-airflow-cs280")

    # First we need to convert the strings we download into a buffer
    # This is necessary since pandas.read_csv requires a path or a buffer
    user_data_csv_buffer = StringIO(bucket.blob("data/user_data.csv").download_as_text())
    tweet_data_csv_buffer = StringIO(bucket.blob("data/tweet_data.csv").download_as_text())

    # Now we can create the dataframes
    usersDF = pd.read_csv(user_data_csv_buffer)
    tweetsDF = pd.read_csv(tweet_data_csv_buffer)

    date_today = pendulum.now('UTC').date()

    # Open a sqlalchemy session
    session = Session()

    # Add new user timeseries entries
    for _, row in usersDF.iterrows():
        user_timeseries = User_Timeseries(
            user_id = row["user_id"],
            followers_count = row["followers_count"],
            following_count = row["following_count"],
            tweet_count = row["tweet_count"],
            listed_count = row["listed_count"],
            date = date_today
        )
        logging.info(user_timeseries.id)
        session.add(user_timeseries)

    # Get the number of tweets already in the database
    originalTweetCount = ti.xcom_pull(key="originalTweetCount", task_ids="load_data_task")

    # Add new tweets and timeseries
    tweetsDF = tweetsDF.sort_values(by=["id"])
    for _, row in tweetsDF.iterrows():
        id = int(row["id"])

        # Add timeseries data
        tweet_timeseries = Tweet_Timeseries(
            tweet_id = row["tweet_id"],
            retweet_count = row["retweet_count"],
            like_count = row["like_count"],
            date = date_today,
        )
        session.add(tweet_timeseries)

        # Add new tweets
        if id > originalTweetCount:
            tweet = Tweet(
                tweet_id = row["tweet_id"],
                user_id = row["user_id"],
                text = row["text"],
                created_at = row["created_at"]
            )
            session.add(tweet)


    # Save database changes and close
    session.commit()
    session.close()


with DAG(
    dag_id="project_lab_2_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 2, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data
    )
    call_api_task = PythonOperator(
        task_id="call_api_task",
        python_callable=call_api
    )
    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data
    )
    write_data_task = PythonOperator(
        task_id="write_data_task",
        python_callable=write_data
    )

load_data_task >> call_api_task >> transform_data_task >> write_data_task