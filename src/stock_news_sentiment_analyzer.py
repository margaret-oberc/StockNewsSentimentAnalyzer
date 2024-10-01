import pandas as pd
import datetime
import os
import pytz
import yfinance as yf
import openai
import mysql.connector
import utils.trading_date_lookup as td
from utils.holiday_manager import load_holiday_dates_from_csv
from pydantic import BaseModel, Field
from typing import Iterable, Optional
from langchain_community.document_loaders.web_base import WebBaseLoader
from langchain_core.documents import Document
from pathlib import Path

# List of stock tickers to analyze
tickers = [ 'AQN', 'BCE', 'PAAS', 'ENB', 'CM', 'BMO', 'TD', 'RY', 'BNS']

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
Estimate sentiment score for {stock} stock from the news article: negative=-1, neutral=0, positive=1.
Provide answer as an integer number and a short comment indicating the reason.
If article does not contain mention of the specific stock, please rate neutral.
Article: {article}
Detect if the article is a financial statement - provide answer as type 'story' or 'fs' for financial statement.
A financial statement is a report issued by a company summarizing financial performance for the last quarter or year.
"""

# Ensure holidays are loaded at initialization
root_dir = Path(__file__).parent.parent
file_path = root_dir / 'data' / 'tsx_holidays.csv'
load_holiday_dates_from_csv(file_path)

# Function to send a sentiment analysis request to OpenAI
def get_sentiment_analysis(stock: str, article: str) -> Optional[SentimentAnswer]:
    try:
        # Create the user message based on the template
        user_message = sentiment_template.format(stock=stock, article=article)
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
    publisher = row['publisher']
    link = row['link']
    news_ts = pd.to_datetime(row['est_time'])
    trading_dt = td.get_trading_date(news_ts) #convert to correct trading date
    news_type = row['type']
    sentiment_score = row['score']
    comment = row['comment']

    query = """
    INSERT INTO ynews (uuid, symbol, news_ts, trading_dt, title, link, publisher, news_type, sentiment_score, comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = (uuid, symbol, news_ts, trading_dt, title, link, publisher, news_type, sentiment_score, comment)
    
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
def unix_to_est(unix_time: int) -> str:
    try:
        # Convert Unix time to UTC datetime
        utc_time = datetime.datetime.fromtimestamp(unix_time, datetime.timezone.utc)
        est = pytz.timezone('America/New_York')
        est_time = utc_time.astimezone(est)
        return est_time.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error converting Unix time: {e}")
        return None

# Helper function to format a list of Document objects into a string
def format_results(docs: Iterable[Document]) -> str:
    doc_strings = []
    for doc in docs:
        title = doc.metadata.get("title", "No Title Available")
        description = doc.metadata.get("description", "No Description Available")
        doc_strings.append(f"{title}\n{description}")
    return "\n\n".join(doc_strings)

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
        stock = yf.Ticker(symbol)
        print(f"\nProcessing {stock.ticker}")

        try:
            stock_info = stock.info
            stock_long_name = stock_info.get('longName')
            resp = stock.news
            if not resp:
                print(f"No news available for {symbol}.")
                continue
            data = pd.DataFrame(resp)
            data['est_time'] = data['providerPublishTime'].apply(unix_to_est)
            print(data[['title', 'publisher', 'est_time']])
        except Exception as e:
            print(f"Error fetching news for {symbol}: {e}")
            continue

        data['score'] = None
        data['type'] = None
        data['comment'] = None

        for index, row in data.iterrows():
            uuid = row['uuid']
            if article_exists(connection, uuid):
                continue

            link = row['link']
            try:
                loader = WebBaseLoader(web_paths=[link])
                docs = loader.load()
                article = format_results(docs)
            except Exception as e:
                print(f"Error loading article from {link}: {e}")
                continue

            if not article:
                print(f"No article content found for {link}.")
                continue

            sentiment = get_sentiment_analysis(symbol, article)
            if sentiment:
                row['score'] = sentiment.score
                row['type'] = sentiment.type
                row['comment'] = sentiment.comment


                insert_ynews(row, connection, symbol)

    # Close DB connection
    connection.close()

if __name__ == "__main__":
    main()
