import socket
import json
import time
from confluent_kafka import Producer
import praw
from pymongo import MongoClient
import re
import pandas as pd
from datetime import datetime
import schedule

mongo_atlas_connection_string = 'mongodb+srv://admin:admin@cluster0.rfwg72h.mongodb.net/?retryWrites=true&w=majority'

# Create a client
client = MongoClient(mongo_atlas_connection_string)

# Access the 'reddit_data' database
db = client['reddit_data']

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}
producer = Producer(conf)

# Function to initialize the Reddit API client
def initialize_reddit_client():
    reddit = praw.Reddit(
        client_secret='QoYI56Cq5B56PiDI7zddmk2snE0dsg',
        client_id='_mHJu3-2NDl8VJuRg-tkkQ',
        user_agent='ADT_Project'
    )
    return reddit

# Function to clean text columns in DataFrame
def clean_text_column(df, column_name):
    df['clean_' + column_name] = df[column_name].apply(lambda x: re.split('https:\/\/.*', str(x))[0])
    df['clean_' + column_name] = df['clean_' + column_name].str.replace('[^\w\s]', '')
    df['clean_' + column_name] = df['clean_' + column_name].replace('\n', '', regex=True)
    df['clean_' + column_name] = df['clean_' + column_name].replace('  ', '', regex=True)
    return df



# Function to convert a datetime column in DataFrame
def convert_datetime_column(df, datetime_column_name):
    df[datetime_column_name] = pd.to_datetime(df[datetime_column_name], errors='coerce')
    df['reddit_date'] = df[datetime_column_name].dt.date.astype(str)  # Convert date to string
    df['reddit_time'] = df[datetime_column_name].dt.time.astype(str)  # Convert time to string
    return df



# Function to extract month and year from datetime column in DataFrame
def extract_month_year(df, datetime_column_name):
    df['Month'] = df[datetime_column_name].dt.month
    df['Year'] = df[datetime_column_name].dt.year

# Function to store Reddit data in MongoDB and Kafka
def store_reddit_data(df):
    reddit_collection = db['reddit_data']
    records = df.to_dict(orient='records')
    reddit_collection.insert_many(records)

    # Send Reddit data to Kafka
    for record in records:
        # Convert ObjectId to string before serialization
        record['_id'] = str(record['_id'])
        record['DateTime_UTC'] = str(record['DateTime_UTC'])
        producer.produce('reddit_data', value=json.dumps(record), callback=delivery_report)


def fetch_reddit_posts(reddit, subreddits, start_of_2021, end_of_2021):
    data = []

    for subreddit_name in subreddits:
        subreddit = reddit.subreddit(subreddit_name)
        limit = 10

        for submission in subreddit.top(limit=limit):
            if start_of_2021 <= submission.created_utc <= end_of_2021:
                data.append({
                    'Subreddit_Name': subreddit_name,
                    'Title': submission.title,
                    'Author': str(submission.author),
                    'SelfText': submission.selftext,
                    'Score': submission.score,
                    'NoOfComments': submission.num_comments,
                    'NSFW': submission.over_18,
                    'URL': submission.url,
                    'DateTime_UTC': datetime.utcfromtimestamp(submission.created_utc),
                    'UpvoteRatio': submission.upvote_ratio
                })

    df = pd.DataFrame(data)
    convert_datetime_column(df, 'DateTime_UTC')
    extract_month_year(df, 'DateTime_UTC')
    clean_text_column(df, 'Title')
    clean_text_column(df, 'Author')
    clean_text_column(df, 'SelfText')

    return df

# Main function to run Reddit data fetching
def main():
    print("Fetching Reddit data")
    # Fetch Reddit posts
    reddit = initialize_reddit_client()
    subreddits = ['dataisbeautiful', 'Instagram', 'Music','LetsTalkMusic','Movies','entertainment','FashionReps']
    start_of_2021 = datetime(2021, 1, 1).timestamp()
    end_of_2021 = datetime(2023, 5, 30, 23, 59, 59).timestamp()
    reddit_df = fetch_reddit_posts(reddit, subreddits, start_of_2021, end_of_2021)
    store_reddit_data(reddit_df)

# Schedule the main function to run every 30 minutes
schedule.every(5).minutes.do(lambda: main())

# Infinite loop to check scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(5)
