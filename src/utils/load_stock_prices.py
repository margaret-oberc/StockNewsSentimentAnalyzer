import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from decimal import Decimal
from dateutil.relativedelta import relativedelta
import trading_date_lookup as td

def upload_stock_data_to_db(connection, symbol, start_date, end_date=None):
    cursor = connection.cursor()

    # Download stock data from yfinance
    stock_data = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False)
    stock_data = stock_data.droplevel(level=1, axis=1)
        
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

    # Convert price-related columns to Decimal and round to 2 decimal places
    for col in ['open_price', 'high', 'low', 'close_price', 'adj_close_price']:
        stock_data[col] = stock_data[col].apply(lambda x: round(Decimal(str(x)), 2))

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
            print(f"Error inserting data for {symbol} on {row['close_dt']}: {e}")

    # Commit the transaction and close the cursor
    connection.commit()
    cursor.close()

def store_fundamentals_to_db(connection, symbol, trading_date):
    cursor = connection.cursor()
    try:
        info = yf.Ticker(symbol).info
        cursor.execute("""
            INSERT INTO fundamentals (
                symbol, close_dt, trailingPE, forwardPE, priceToBook,
                debtToEquity, returnOnEquity, profitMargins, marketCap,
                beta, dividendYield, earningsQuarterlyGrowth
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                trailingPE = VALUES(trailingPE),
                forwardPE = VALUES(forwardPE),
                priceToBook = VALUES(priceToBook),
                debtToEquity = VALUES(debtToEquity),
                returnOnEquity = VALUES(returnOnEquity),
                profitMargins = VALUES(profitMargins),
                marketCap = VALUES(marketCap),
                beta = VALUES(beta),
                dividendYield = VALUES(dividendYield),
                earningsQuarterlyGrowth = VALUES(earningsQuarterlyGrowth)
        """, (
            symbol,
            trading_date,
            info.get("trailingPE"),
            info.get("forwardPE"),
            info.get("priceToBook"),
            info.get("debtToEquity"),
            info.get("returnOnEquity"),
            info.get("profitMargins"),
            info.get("marketCap"),
            info.get("beta"),
            info.get("dividendYield"),
            info.get("earningsQuarterlyGrowth"),
        ))
    except Exception as e:
        print(f"Error inserting fundamentals for {symbol}: {e}")
    connection.commit()
    cursor.close()

def main():
    # List of stock tickers to analyze
    tickers = ['^GSPTSE', 'ACO-X.TO', 'AEM.TO', 'ALA.TO', 'ALC.TO', 'AQN', 'BCE', 'BMO', 'BNS', 'CM', 'CP', 'CU.TO', 'EMA.TO', 'ENB', 'FC.TO', 'FTS', 'GWO.TO', 'H.TO', 'IFC.TO', 'KEY.TO', 'L.TO', 'MFC',
                'MKP.TO', 'MRU.TO', 'NA.TO', 'PAAS', 'POW.TO', 'PPL.TO', 'PXT.TO', 'QBR-B.TO', 'RCI-B.TO', 'RSI.TO', 'RY', 'SIA.TO', 'SLF.TO', 'SU', 'T.TO', 'TD', 'TRI', 'USA.TO', 'WCN.TO', 'WN.TO', 'X.TO']
    start_dates = ['2017-01-01', '2018-01-01', '2019-01-01', '2020-01-01', '2021-01-01', '2022-01-01', '2023-01-01', '2024-01-01', '2025-01-01']
    
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user="moberc",
            password="moberc",
            database="investments"
        )
    except Exception as e:
        print("Error connecting to database:", e)
        return

    trading_date = td.get_last_trading_date(datetime.now())

    for symbol in tickers:
        print(f"Processing {symbol}")

        to_symbol = f"{symbol}.TO" if not (symbol.endswith('.TO') or symbol.startswith('^')) else symbol

        for start_date in start_dates:
            # Adjust end_date to be inclusive
            end_date = (datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(years=1) - timedelta(days=1)).strftime("%Y-%m-%d")
            upload_stock_data_to_db(connection, to_symbol, start_date, end_date)
    
        # Store fundamentals for end_date
        if symbol.startswith('^'):
            continue  
        try:
            store_fundamentals_to_db(connection, to_symbol, trading_date)
        except Exception as e:
            print(f"Error fetching fundamentals for {to_symbol}: {e}")

    connection.close()

if __name__ == "__main__":
    main()
