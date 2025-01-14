import pandas as pd
import datetime
import os
import pytz
import feedparser
import openai
import mysql.connector
import utils.trading_date_lookup as td
from utils.holiday_manager import load_holiday_dates_from_csv
from pydantic import BaseModel, Field
from typing import Iterable, Optional
from pathlib import Path

# List of stock tickers to analyze
tickers = ['AQN', 'FC.TO', 'BCE', 'PAAS', 'ENB', 'CM', 'BMO', 'TD', 'RY', 'MFC', 'BNS', 'CP', 'TRI', 'SU', 'AEM', 'L.TO']

# Pydantic model to structure sentiment response
class SentimentAnswer(BaseModel):
    score: int = Field(description="Sentiment score: negative=-1, neutral=0, positive=1")
    type: str = Field(description="Type of article: story or fs (financial statement)")
    comment: str = Field(description="Explanation for the sentiment score")

# OpenAI API setup
openai_api_key = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = 'ft:gpt-4o-mini-2024-07-18:personal::A5cBFbkn'
client = openai.OpenAI()

# Template to request sentiment analysis from the OpenAI model
sentiment_template = """
Estimate sentiment score for {symbol} stock from the news article: negative=-1, neutral=0, positive=1.
Provide answer as an integer number and a short comment indicating the reason.
If article does not contain mention of the specific stock, please rate neutral.
Title: {title}
Article: {article}
Detect if the article is a financial statement - provide answer as type 'story' or 'fs' for financial statement.
A financial statement is a report issued by that company summarizing financial performance for the last quarter or year.
"""
# Ensure holidays are loaded at initialization
root_dir = Path(__file__).parent.parent
file_path = root_dir / 'data' / 'tsx_holidays.csv'
load_holiday_dates_from_csv(file_path)

# Function to get news for the given symbol
def get_news(symbol: str):

    # URL of the RSS feed
    rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=USA&lang=en-US&count=500"

    # Parse the RSS feed
    feed = feedparser.parse(rss_url)

    # Check if the feed was successfully parsed
    if feed.bozo:
        print("Failed to parse the RSS feed.")
        return None

    news_items = []
    # Loop through each news item and collect details
    for entry in feed.entries:
        news_items.append({
            'uuid': entry.id,
            'title': entry.title,
            'link': entry.link,
            'publication date': entry.published,
            'description': entry.description
        })

    # Convert the list of news items into a DataFrame
    news_df = pd.DataFrame(news_items)

    # Convert the 'Publication Date' to datetime
    news_df['publication date'] = pd.to_datetime(news_df['publication date'])

    return news_df
    rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US&count=500"

    # Parse the RSS feed
    feed = feedparser.parse(rss_url)

    # Check if the feed was successfully parsed
    if feed.bozo:
        print("Failed to parse the RSS feed.")
        return None

    news_items = []
    # Loop through each news item and collect details
    for entry in feed.entries:
        news_items.append({
            'uuid': entry.id,
            'title': entry.title,
            'link': entry.link,
            'publication date': entry.published,
            'description': entry.description
        })

    # Convert the list of news items into a DataFrame
    news_df = pd.DataFrame(news_items)

    # Convert the 'Publication Date' to datetime
    news_df['publication date'] = pd.to_datetime(news_df['publication date'])

    return news_df

# Function to send a sentiment analysis request to OpenAI
def get_sentiment_analysis(symbol: str, title: str, article: str) -> Optional[SentimentAnswer]:
    try:
        # Create the user message based on the template
        user_message = sentiment_template.format(symbol=symbol, title=title, article=article)
        messages = [
            {"role": "system", "content": "You are a financial news sentiment analysis assistant."},
            {"role": "user", "content": user_message}
        ]

        # Request the sentiment from OpenAI and parse the structured response
        completion = client.beta.chat.completions.parse(
            model=OPENAI_MODEL,
            messages=messages,
            response_format=SentimentAnswer,
        )
        return completion.choices[0].message.parsed
    except Exception as e:
        print(f"Error fetching sentiment analysis: {e}")
        return None

# SQL: Check if an article exists in the database by its UUID
def article_exists(connection, uuid: str) -> bool:
    try:
        with connection.cursor() as cursor:
            sql = "SELECT 1 FROM ynews WHERE uuid = %s LIMIT 1;"
            cursor.execute(sql, (uuid,))
            result = cursor.fetchone()
            return result is not None
    except Exception as e:
        print(f"Error checking if entry exists: {e}")
        return False

# SQL: Insert a new news article into the database
def insert_ynews(row: pd.Series, connection, symbol: str):
    uuid = row['uuid']
    title = row['title']
    description = row['description']
    link = row['link']
    news_ts = pd.to_datetime(row['est_time'])
    trading_dt = td.get_trading_date(news_ts) #convert to correct trading date
    news_type = row['type']
    sentiment_score = row['score']
    comment = row['comment']

    query = """
    INSERT INTO ynews(uuid, symbol, news_ts, trading_dt, title, link, description, news_type, sentiment_score, comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = (uuid, symbol, news_ts, trading_dt, title, link, description, news_type, sentiment_score, comment)
    
    cursor = connection.cursor()
    try:
        cursor.execute(query, data)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error inserting news: {err}")
        connection.rollback()
    finally:
        cursor.close()

# Helper function to convert Unix timestamp to EST
def utc_to_est(utc_time: datetime) -> str:
    try:
        est = pytz.timezone('America/New_York')
        est_time = utc_time.astimezone(est)
        return est_time.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error converting Unix time: {e}")
        return None

# Main: Establish connection to MySQL database
def main():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            user="moberc",
            password="moberc",
            database="investments",
            auth_plugin="mysql_native_password"
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return

    # Process news for each stock symbol
    for symbol in tickers:
        print(f"\nProcessing {symbol}")

        try:
            data = get_news(symbol)
            if data is None:
                print(f"No news available for {symbol}.")
                continue
            data['est_time'] = data['publication date'].apply(utc_to_est)
            print(data[['title', 'est_time']])
        except Exception as e:
            print(f"Error fetching news for {symbol}: {e}")
            continue

        data['score'] = None
        data['type'] = None
        data['comment'] = None

        for index, row in data.iterrows():
            uuid = row['uuid']
            title = row['title']
            article = row['description']

            # skip if already processed
            if article_exists(connection, uuid):
                continue

            sentiment = get_sentiment_analysis(symbol, title, article)
            if sentiment:
                row['score'] = sentiment.score
                row['type'] = sentiment.type
                row['comment'] = sentiment.comment

                insert_ynews(row, connection, symbol)

    # Close DB connection
    connection.close()

if __name__ == "__main__":
    main()
