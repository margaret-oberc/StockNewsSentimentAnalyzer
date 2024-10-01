import pymysql
import math
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import statsmodels.api as sm

def get_prev_business_dt(symbol, trading_dt, db_connection):
    """
    Fetch the previous business day for a given symbol and trading date.
    """
    if isinstance(trading_dt, str):
        trading_dt = datetime.strptime(trading_dt, '%Y-%m-%d')
    
    query = """
        SELECT max(close_dt)
        FROM stock_price
        WHERE symbol = %s AND close_dt < %s
    """

    with db_connection.cursor() as cursor:
        cursor.execute(query, (symbol, trading_dt))
        prev_dt = cursor.fetchone()
        
        if prev_dt is None or len(prev_dt) == 0:
            raise ValueError(f"Not enough data to calculate price change for {symbol} around {trading_dt}")
    
    return prev_dt[0]


def get_stock_price_change(symbol, trading_dt, db_connection):
    """
    Get stock price changes for a given symbol and trading date.
    """
    prev_dt = get_prev_business_dt(symbol, trading_dt, db_connection)
    
    query = """
        SELECT adj_close_price
        FROM stock_price
        WHERE symbol = %s AND close_dt >= %s
        ORDER BY close_dt ASC
        LIMIT 6;
    """

    with db_connection.cursor() as cursor:
        cursor.execute(query, (symbol, prev_dt))
        rows = cursor.fetchall()

        if len(rows) < 6:
            return None  # not enough data

        adj_close_prices = [row[0] for row in rows]
        previous_price = adj_close_prices[0]

        price_changes = [
            round(100 * (adj_close_prices[i] - previous_price) / previous_price, 4)
            for i in range(1, len(adj_close_prices))
        ]
    return adj_close_prices, price_changes

def process_ticker(symbol, connection, tsx_symbol):
    """
    Process a single ticker and fetch the necessary stock price and news data.
    """
    data = []
    query = """
        SELECT symbol, trading_dt, 
        COALESCE(MIN(CASE WHEN sentiment_score < 0 THEN sentiment_score END), 0) AS min_score,
        COALESCE(MAX(CASE WHEN sentiment_score > 0 THEN sentiment_score END), 0) AS max_score 
        FROM ynews
        WHERE symbol = %s AND news_type <> 'fs'
        GROUP BY symbol, trading_dt
        ORDER BY symbol, trading_dt
    """
    
    to_symbol = f"{symbol}.TO" if not symbol.endswith('.TO') else symbol

    with connection.cursor() as cursor:
        cursor.execute(query, (symbol,))
        news_items = cursor.fetchall()

        for news in news_items:
            symbol, trading_dt, min_score, max_score = news
            result = get_stock_price_change(to_symbol, trading_dt, connection)
            
            if result is None:
                continue

            adj_close_prices, price_changes = result
            tsx_result = get_stock_price_change(tsx_symbol, trading_dt, connection)

            if tsx_result is None:
                continue

            tsx_adj_close_prices, tsx_price_changes = tsx_result
            data.append([
                symbol, trading_dt, min_score, max_score, adj_close_prices[1],
                price_changes[0], price_changes[1], price_changes[2], price_changes[3], price_changes[4],
                tsx_price_changes[0], tsx_price_changes[1], tsx_price_changes[2], tsx_price_changes[3], tsx_price_changes[4]
            ])

    return data


def fit_OLS(data):
    """
    Fit an OLS model to the data and print a summary of the results.
    """
    # Convert data to DataFrame
    df = pd.DataFrame(data, columns=['symbol', 'trading_dt', 'min_score', 'max_score',
    'price', 'price_change', 't1_price_change', 't2_price_change', 't3_price_change', 't4_price_change',
    'tsx_price_change', 'tsx_t1_price_change', 'tsx_t2_price_change', 'tsx_t3_price_change', 'tsx_t4_price_change'
    ])

    # Ensure that numeric columns are in the correct format
    df['price_change'] = pd.to_numeric(df['price_change'], errors='coerce')
    df['tsx_price_change'] = pd.to_numeric(df['tsx_price_change'], errors='coerce')

    # Drop any rows with missing values (optional, if applicable)
    df = df.dropna()

    # Define independent variables  
    X = df[['min_score', 'max_score', 'tsx_price_change']]
    y = df['price_change']

    # Split the data into training and testing sets (80% train, 20% test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Fit the linear regression model using statsmodels to get coefficient significance
    model = sm.OLS(y_train, X_train).fit()

    # Get the predictions
    y_pred = model.predict(X_test)

    # Evaluate the model performance using r-squared
    r2 = r2_score(y_test, y_pred)
    print(f"R-squared on test data: {r2}")

    # Print the full statistical summary of the model
    print(model.summary())


def main():
    # Database connection details
    db_config = {
        'database': 'investments',
        'user': 'moberc',
        'password': 'moberc',
        'host': 'localhost',
        'port': 3306
    }

    # List of stock tickers to analyze
    tickers = [ 'AQN', 'BCE', 'PAAS', 'ENB', 'CM', 'BMO', 'TD', 'RY', 'BNS']
    TSX = '^GSPTSE'

    with pymysql.connect(**db_config) as connection:
        for symbol in tickers:
            print(f"Processing {symbol}")

            data = process_ticker(symbol, connection, TSX)

            if data:
                fit_OLS(data)


if __name__ == "__main__":
    main()
