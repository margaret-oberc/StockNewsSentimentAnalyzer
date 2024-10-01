from datetime import datetime, timedelta
from utils.holiday_manager import load_holiday_dates_from_csv, is_holiday

def get_next_trading_day(current_date):
    """
    Get the next trading day excluding weekends and holidays.
    
    :param current_date: The current date
    :return: The next valid trading day as a datetime.date object
    """
    next_date = current_date + timedelta(days=1)
    while next_date.weekday() >= 5 or is_holiday(next_date):  # Skip weekends and holidays
        next_date += timedelta(days=1)
    
    return next_date

def get_trading_date(timestamp_est):
    """
    Get the correct trading day for a given timestamp.
    
    :param timestamp_est: A datetime object in EST
    :return: The next valid trading day as a datetime.date object
    """
    four_pm_est = timestamp_est.replace(hour=16, minute=0, second=0, microsecond=0)
    
    if timestamp_est < four_pm_est:
        trading_date = timestamp_est.date()  # Use current date
    else:
        trading_date = timestamp_est.date() + timedelta(days=1)  # Use next day
    
    # Check if the computed date is a valid trading day
    if trading_date.weekday() >= 5 or is_holiday(trading_date):  # Weekend or holiday
        return get_next_trading_day(trading_date)
    
    return trading_date