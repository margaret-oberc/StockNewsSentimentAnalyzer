# Stock News Sentiment Analyzer

This Python project analyzes stock news articles to provide sentiment scores and categorize financial reports. It automates fetching news for a list of stock tickers, running sentiment analysis using a fine-tuned OpenAI model, and storing the results in a MySQL database.

## Features
- **Stock News Fetching**: Fetches stock news articles for a set of stock tickers.
- **Sentiment Analysis**: Uses a fine-tuned OpenAI model for sentiment analysis.
- **Database Storage**: Stores the sentiment analysis results in a MySQL database.
- **Price Analysis**: Correlates stock price movements with sentiment scores using regression models.

## Prerequisites
- Python 3.8+
- MySQL database
- OpenAI API key
- A CSV file with trading holidays

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/moberc/StockNewsSentimentAnalyzer.git
    cd StockNewsSentimentAnalyzer
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Set up the MySQL database using the table definitions in the `README.md`.

4. Run the project:
    ```bash
    python src/main_sentiment_analyzer.py
    ```

## Database Setup

Ensure the following tables exist in your MySQL database:

```sql
CREATE TABLE IF NOT EXISTS ynews(
    uuid VARCHAR(36) PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    news_ts TIMESTAMP,
    trading_dt DATE,
    title VARCHAR(1024),
    link VARCHAR(1024),
    publisher VARCHAR(256),
    news_type VARCHAR(16),
    sentiment_score int,
    comment varchar(1024)
);
CREATE INDEX ynews_idx on ynews (symbol, news_dt);

CREATE TABLE IF NOT EXISTS stock_price(
    symbol VARCHAR(10) NOT NULL,
    close_dt DATE,
    open_price DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close_price DECIMAL(20,6),
    adj_close_price DECIMAL(20,6),
    volume BIGINT,
    PRIMARY KEY (symbol, close_dt)
);

