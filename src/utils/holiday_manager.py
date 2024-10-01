import pandas as pd
from datetime import datetime

# Declare the holiday_cache as a global variable
holiday_cache = set()

def load_holiday_dates_from_csv(csv_file_path):
    """
    Reads a CSV file with holiday dates and loads them into a cache.
    
    :param csv_file_path: Path to the CSV file
    """
    global holiday_cache  # Declare that we're modifying the global variable
    
    # Read the CSV file into a DataFrame
    calendar_df = pd.read_csv(csv_file_path)
    
    # Convert holiday_date column to datetime.date objects
    calendar_df['holiday_date'] = pd.to_datetime(calendar_df['holiday_date']).dt.date
    
    # Populate the global holiday_cache set with the holiday dates
    holiday_cache = set(calendar_df['holiday_date'])
    print(f"Loaded holidays: {len(holiday_cache)}")

def is_holiday(date):
    """
    Check if the given date is a holiday.
    
    :param date: A datetime.date or datetime object.
    :return: True if the date is a holiday, False otherwise.
    """
    if isinstance(date, datetime):
        date = date.date()  # Extract the date if a datetime object is passed
    
    return date in holiday_cache