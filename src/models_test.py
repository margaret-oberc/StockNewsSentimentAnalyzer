import pandas as pd
import datetime
import os
import pytz
import feedparser
import openai
import utils.trading_date_lookup as td
from utils.holiday_manager import load_holiday_dates_from_csv
from pydantic import BaseModel, Field
from typing import Optional
from pathlib import Path

# List of stock tickers to analyze
tickers = ['AEM.TO']

sentiment_template = """
Estimate sentiment score for {symbol} stock from the news article: negative=-1, neutral=0, positive=1.
Provide answer as an integer number and a short comment indicating the reason.

Target company: {symbol}
Title: {title}
Article: {article}

Classification rules:

Identify information specifically relevant to the target company, regardless of how prominently it appears in the article.
The target company may be the main subject, a secondary subject, or mentioned only briefly.
If the target company is mentioned briefly but the information has material implications for its business, financial performance, competitive position, or valuation, evaluate that information.
Do not automatically transfer sentiment from the main company to the target company.
If the target company is mentioned only as a competitor, industry participant, or in a list of peers, assign neutral unless the article provides a clear implication for the target company.
If the article contains no meaningful information about the target company, assign neutral.
Consider financial results relative to expectations when explicitly available.
Base your assessment exclusively on information provided in the article. Do not speculate.
Detect if the article is a financial statement - provide answer as type 'story' or 'fs' for financial statement.
A financial statement is a report issued by that company summarizing financial performance for the last quarter or year.
Return:
- score: -1, 0, or 1
- type: "story" or "fs"
- comment: maximum 20 words
"""

# Pydantic model to structure sentiment response
class SentimentAnswer(BaseModel):
    score: int = Field(description="Sentiment score: negative=-1, neutral=0, positive=1")
    type: str = Field(description="Type of article: story or fs (financial statement)")
    comment: str = Field(description="Explanation for the sentiment score")

# OpenAI API setup
openai_api_key = os.getenv('OPENAI_API_KEY')
client = openai.OpenAI()

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
    #news_df['publication date'] = pd.to_datetime(news_df['publication date'])
    news_df['publication date'] = pd.to_datetime(
    news_df['publication date'],
    format='%a, %d %b %Y %H:%M:%S %z',
    errors='coerce')

    return news_df

def get_sentiment(symbol: str, title: str, article: str) -> Optional[SentimentAnswer]:
    OPENAI_MODEL = 'ft:gpt-4o-mini-2024-07-18:personal::A5cBFbkn'

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


# Function to send a sentiment analysis request to OpenAI
def get_decision_sentiment(symbol, title, article):

    response = client.decisions.create(
        model="gpt-6-luna",

        input=f"""
        Stock: {symbol}
        Title: {title}
        Article: {article}
        """,

        questions=[
            {
                "type": "choice",
                "name": "sentiment",
                "instructions": (
                    "Evaluate the implications of this article "
                    "for the specified stock. "
                    "Negative means materially adverse news; "
                    "positive means materially beneficial news. "
                    "Use neutral for mixed, immaterial, unrelated "
                    "or insufficient information. Consider "
                    "expectations when explicitly available."
                ),
                "choices": [
                    {"value": "negative"},
                    {"value": "neutral"},
                    {"value": "positive"}
                ]
            },
            {
                "type": "choice",
                "name": "article_type",
                "instructions": (
                    "Is this a company-issued quarterly or annual "
                    "financial performance report (fs), "
                    "or another type of article (story)?"
                ),
                "choices": [
                    {"value": "story"},
                    {"value": "fs"}
                ]
                            }
        ]
    )

    answers = {a.name: a for a in response.answers}

    sentiment = answers["sentiment"].choice
    article_type = answers["article_type"].choice

    score_map = {
        "negative": -1,
        "neutral": 0,
        "positive": 1
    }

    return {
        "score": score_map[sentiment],
        "type": article_type,
        "confidence": answers["sentiment"].confidence,
        "probabilities": answers["sentiment"].probabilities
    }


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
    # load test data set
    os.chdir('C:/Projects/StockNewsSentimentAnalyzer-1/data')
    test_sentiment_df = pd.read_csv("sentiment.csv")
 
    # Process news for each stock symbol

    news_comparison_items = []
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

        for index, row in data.iterrows():
            uuid = row['uuid']
            title = row['title']
            link = row['link']
            article = row['description']
            est_time = row['est_time']

            decision_sentiment = get_decision_sentiment(symbol, title, article)
            news_type1 = decision_sentiment['type']
            sentiment_score1 = decision_sentiment['score']
            confidence1 = decision_sentiment['confidence']

            # new template 
            new_sentiment = get_sentiment(symbol, title, article)            
            news_type2 = new_sentiment.type
            sentiment_score2 = new_sentiment.score
            comment2 = new_sentiment.comment

            # current classification
            test_sentiment = test_sentiment_df.loc[test_sentiment_df['uuid'] == uuid]
            first_match = test_sentiment.iloc[0]            
            news_type3 = first_match['news_type']
            sentiment_score3 = first_match['sentiment_score']
            comment3 = first_match['comment']

            news_comparison_items.append({
                'uuid': uuid,
                'title': title,
                'est_time': est_time,
                "link": link,
                'n_score': sentiment_score2,
                'n_type': news_type2,
                'n_comment': comment2,
                'o_score': sentiment_score3,
                'o_type': news_type3,
                'o_comment': comment3,
                'decision_score': sentiment_score1,
                'decision_type': news_type1,
                'decision_confidence': confidence1,
                })
            
    comparison_df = pd.DataFrame(news_comparison_items)
    comparison_df.to_csv("AI comparison.csv", index=False, sep=',', encoding='utf-8')


if __name__ == "__main__":
    main()
