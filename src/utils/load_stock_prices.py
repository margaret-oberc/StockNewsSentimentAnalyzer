import yfinance as yf
import pandas as pd
import pymysql

def upload_stock_data_to_db(connection, symbol, start_date):
    cursor = connection.cursor()

    # Download stock data from yfinance
    stock_data = yf.download(symbol, start=start_date, end=None)
        
    # Prepare the data for insertion
    stock_data.reset_index(inplace=True)
    stock_data['Symbol'] = symbol
    stock_data.rename(columns={
        'Date': 'close_dt',
        'Open': 'open_price',
        'High': 'high',
        'Low': 'low',
        'Close': 'close_price',
        'Adj Close': 'adj_close_price',
        'Volume': 'volume'
    }, inplace=True)

    # Insert each row into the database
    for index, row in stock_data.iterrows():
        try:
            cursor.execute("""
                INSERT INTO stock_price (symbol, close_dt, open_price, high, low, close_price, adj_close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open_price = VALUES(open_price),
                    high = VALUES(high),
                    low = VALUES(low),
                    close_price = VALUES(close_price),
                    adj_close_price = VALUES(adj_close_price),
                    volume = VALUES(volume)
                """, (
                    row['Symbol'], 
                    row['close_dt'], 
                    row['open_price'], 
                    row['high'], 
                    row['low'], 
                    row['close_price'], 
                    row['adj_close_price'], 
                    row['volume']
                ))
        except Exception as e:
            print(f"Error inserting data for {symbol} on {row['close_date']}: {e}")

    # Commit the transaction and close the cursor
    connection.commit()
    cursor.close()


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
    tickers = [ '^GSPTSE', 'AQN', 'BCE', 'PAAS', 'ENB', 'CM', 'BMO', 'TD', 'RY', 'BNS']
    start_date = '2023-09-01'

    with pymysql.connect(**db_config) as connection:
        for symbol in tickers:
            print(f"Processing {symbol}")

            upload_stock_data_to_db(connection, symbol, start_date)


if __name__ == "__main__":
    main()