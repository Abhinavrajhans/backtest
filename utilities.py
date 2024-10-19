import numpy as np
import pandas as pd
from scipy.stats import norm
from datetime import datetime, timedelta
import calendar
import scipy.stats as si
# utilities.py
# utilities.py
import warnings
# Suppress all warnings globally
warnings.filterwarnings("ignore")

def calculate_historical_volatility(equity_data, lookback_period=252):
    equity_data['Log_Return'] = np.log(equity_data['EQ_Close'] / equity_data['EQ_Close'].shift(1))
    rolling_std = equity_data['Log_Return'].rolling(window=lookback_period).std()
    volatility = rolling_std * np.sqrt(252)  # Annualize the standard deviation
    equity_data['Volatility'] = volatility
    return equity_data

def calculate_greeks(S, K, T, r, sigma, option_type='call'):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    if option_type == 'call':
        delta = si.norm.cdf(d1)
    elif option_type == 'put':
        delta = -si.norm.cdf(-d1)

    return delta

def calculate_time_to_maturity(date):
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    expiry_date = last_thursday_of_month(date_obj)
    days_to_maturity = (expiry_date - date_obj).days
    return days_to_maturity / 365.0

def last_thursday_of_month(date):
    next_month = date.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)
    last_thursday = last_day - timedelta(days=(last_day.weekday() - 3) % 7)
    return last_thursday

def simulate_futures_price(spot_price, time_to_maturity, risk_free_rate=0.07):
    return spot_price * np.exp(risk_free_rate * time_to_maturity)

def get_option_price(options_data, strike_price, option_type='call', ohlc='Close'):
    options_data[['Strike Price', 'Extracted Option Type']] = options_data['Ticker'].apply(extract_strike_price_and_type).apply(pd.Series)
    option_row = options_data[(options_data['Strike Price'] == strike_price) & (options_data['Extracted Option Type'] == option_type)]
    return option_row[ohlc].values[0] if not option_row.empty else None

def extract_strike_price_and_type(ticker):
    parts = ticker.split('-')
    strike_and_type = parts[-1]
    strike_price = ''.join(filter(str.isdigit, strike_and_type))
    option_type = 'call' if 'CE' in strike_and_type else 'put' if 'PE' in strike_and_type else None
    return float(strike_price), option_type

def find_option_by_delta(options_for_date, date, spot_price, time_to_maturity, volatility, target_delta, option_type='call'):
    options_for_date = options_for_date[options_for_date['Extracted Option Type'] == option_type]
    options_for_date.loc[:, 'Calculated_Delta'] = options_for_date.apply(
        lambda row: calculate_greeks(spot_price, row['Strike Price'], time_to_maturity, 0.07, volatility, option_type), axis=1)
    options_for_date.loc[:, 'Delta_Diff'] = abs(options_for_date['Calculated_Delta'] - target_delta)
    return options_for_date.loc[options_for_date['Delta_Diff'].idxmin()]

def calculate_time_to_expiry(current_date_str):
    # Convert string date to datetime object
    current_date = datetime.strptime(current_date_str, "%Y-%m-%d")

    # Find the last Thursday of the current month
    next_month = current_date.replace(day=28) + timedelta(days=4)  # ensures we are in the next month
    last_day_of_month = next_month - timedelta(days=next_month.day)
    last_thursday_current_month = last_day_of_month - timedelta(days=(last_day_of_month.weekday() - 3) % 7)

    # If the current date is after the last Thursday of the current month, calculate expiry in the next month
    if current_date > last_thursday_current_month:
        # Find the last Thursday of the next month
        next_month = last_thursday_current_month + timedelta(days=28)  # Move to next month
        next_month_end = next_month.replace(day=28) + timedelta(days=4)
        last_day_of_next_month = next_month_end - timedelta(days=next_month_end.day)
        last_thursday_next_month = last_day_of_next_month - timedelta(days=(last_day_of_next_month.weekday() - 3) % 7)
        days_to_expiry = (last_thursday_next_month - current_date).days
    else:
        # If we are before the last Thursday of the current month
        days_to_expiry = (last_thursday_current_month - current_date).days
    # Return the time to expiry
    return days_to_expiry