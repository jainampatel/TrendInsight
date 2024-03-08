from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import pandas as pd
import re
from functools import reduce
from pymongo import MongoClient
from datetime import datetime


mongo_atlas_connection_string = 'mongodb+srv://admin:admin@cluster0.rfwg72h.mongodb.net/?retryWrites=true&w=majority'

# Create a client
client = MongoClient(mongo_atlas_connection_string)

# Access the 'reddit_data' database
db = client['reddit_data']

def upload_files():
    Tk().withdraw()  # Prevents the root window from appearing
    file_paths = askopenfilenames()  # Open the file dialog to select multiple files
    return file_paths

# Example usage
file_paths = upload_files()

def loading_csv(files):
    import pandas as pd
    import re
    from functools import reduce

    data_frames = []
    for file_info in files:
        file_path, year, month = file_info
        df = pd.read_csv(file_path)
        df['Year'] = year
        df['Month'] = month
        data_frames.append(df)

    if data_frames:
        # Concatenate the list of data frames into a single data frame
        result_df = pd.concat(data_frames, ignore_index=True)
        return result_df
    else:
        return None

# Dropping unnecessary cols
def drop_columns(df, columns_to_drop):
    for column in columns_to_drop:
      df.drop(columns=column, inplace=True)

def perform_sentimental_analysis(df_all):
    import nltk
    nltk.download('vader_lexicon')
    from nltk.sentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    df = df_all
    df['polarity'] = df['clean_tweet_text'].apply(lambda x: analyzer.polarity_scores(x))
    df['negative'] = df['polarity'].apply(lambda x: x['neg'])
    df['positive'] = df['polarity'].apply(lambda x: x['pos'])
    df['neutral'] = df['polarity'].apply(lambda x: x['neu'])
    df['compound'] = df['polarity'].apply(lambda x: x['compound'])
    df.drop(['polarity'], axis=1)
    df[:3]

# Converting datetime col
def convert_datetime_column(df, datetime_column_name='tweet_datetime'):
    df[datetime_column_name] = pd.to_datetime(df[datetime_column_name], errors='coerce')
    # Handle non-convertible values
    invalid_date_mask = df[datetime_column_name].isnull()
    if invalid_date_mask.any():
        print(f"Warning: Found {invalid_date_mask.sum()} invalid date values. Handling them appropriately.")
        # You can choose to drop the invalid rows, replace with a default value, or handle in a way that makes sense for your use case.
        # For now, let's replace invalid dates with a default date.
        df.loc[invalid_date_mask, datetime_column_name] = pd.to_datetime('1970-01-01')

    df['tweet_date'] = df[datetime_column_name].dt.date
    df['tweet_time'] = df[datetime_column_name].dt.time
    return df

# Cleaning text
def clean_text_column(df, column_name):
  df['clean_' + column_name] = df[column_name].apply(lambda x: re.split('https:\/\/.*', str(x))[0])
  df['clean_' + column_name] = df['clean_' + column_name].str.replace('[^\w\s]', '')
  df['clean_' + column_name] = df['clean_' + column_name].replace('\n','', regex=True)
  df['clean_' + column_name] = df['clean_' + column_name].replace('  ','', regex=True)
  return df


def store_reddit_data(df):
   # Convert datetime.date to datetime.datetime
    df['tweet_datetime'] = df['tweet_datetime'].apply(lambda x: datetime.combine(x, datetime.min.time()))

    # Print rows with problematic values
    problematic_rows = df[df['tweet_datetime'].apply(lambda x: not isinstance(x, datetime))]
    print("Rows with problematic values:")
    print(problematic_rows)

    twitter_collection = db['twitter_data']
    df['clean_tweet_text'] = df['clean_tweet_text'].astype(str)
    df['clean_user_description'] = df['clean_user_description'].astype(str)
    df['clean_searched_by_hashtag'] = df['clean_searched_by_hashtag'].astype(str)
    df.to_csv('/Users/jainampatel/Downloads/TrendInsigt/twitter_data.csv', index=False)
    print(df.dtypes)  
    records = df.to_dict(orient='records')
    for record in records:
      record['tweet_datetime'] = str(record['tweet_datetime'])
      record['tweet_date'] = str(record['tweet_date'])
      record['tweet_time'] = str(record['tweet_time'])

    twitter_collection.insert_many(records)


# Calling the main function
def main():
  file_info_array = []
  for file in file_paths:
    match = re.match(r'.*/(\d{4})_(\w+)_twitter_trending_data\.csv', file)
    if match:
        year = match.group(1)
        month = match.group(2)
        file_info = (file, year, month)
        file_info_array.append(file_info)
   

  df_all = loading_csv(file_info_array)

  df_all = convert_datetime_column(df_all, 'tweet_datetime')

  clean_text_column(df_all,'tweet_text')
  clean_text_column(df_all,'user_description')
  clean_text_column(df_all,'searched_by_hashtag')
  df_all[:13]

  perform_sentimental_analysis(df_all);

  columns_to_remove = ['tweet_id', 'tweet_source', 'tweet_source_url', 'user_created_datetime','polarity','tweet_text','searched_by_hashtag']
  drop_columns(df_all, columns_to_remove)
  return df_all


# for file in file_paths:
#     print("Selected Files:", file)
df = main()
print(df)
store_reddit_data(df)




