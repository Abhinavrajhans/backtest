# options_backtest.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
import scipy.stats as si
import copy

from utilities import calculate_time_to_maturity, calculate_historical_volatility, get_option_price, find_option_by_delta, extract_strike_price_and_type, calculate_time_to_expiry

# options_backtest.py

import warnings
# Suppress only SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)

def backtest_options(stock_ticker, equity_data, options_data, start_date, end_date, total_exposure, dte, sl=1, target_delta=0.25, max_reentries=0, reentry_type = "asap", option_type = "call"):
    option_trades = []
    option_entry_price = None  # Track original entry price for re-entry logic
    re_entry_open = False  # Track re-entry status
    reentry_count = 0  # Count of re-entries
    if (option_type == "put"):
        target_delta = -1*target_delta
    # Calculate historical volatility
    equity_data_vol = calculate_historical_volatility(equity_data)
    equity_data_vol.fillna(0.3, inplace=True)

    is_position_open = False
    option_open = False
    for date in equity_data_vol['Date'].unique():
        try:
            if date > end_date:
                break
            if date < start_date:
                continue

            spot_price = equity_data_vol[(equity_data_vol['Ticker'] == stock_ticker) & (equity_data_vol['Date'] == date)]['EQ_Close'].values[0]
            time_to_maturity = calculate_time_to_maturity(date)
            volatility = equity_data_vol[(equity_data_vol['Ticker'] == stock_ticker) & (equity_data_vol['Date'] == date)]['Volatility'].values[0]
            options_for_date = options_data[options_data['Date'] == date]

            if options_for_date.empty:
                continue

            options_for_date[['Strike Price', 'Extracted Option Type']] = options_for_date['Ticker'].apply(extract_strike_price_and_type).apply(pd.Series)
            #print(date, calculate_time_to_expiry(date), is_position_open)
            # If no position is open, enter the options position
            if not is_position_open and calculate_time_to_expiry(date) <= dte:
                option_target_delta = find_option_by_delta(options_for_date, date, spot_price, time_to_maturity, volatility, target_delta, option_type)
                option_initial_price = option_target_delta['Close']
                option_entry_price = option_initial_price  # Store the original entry price for re-entry
                lot_size = total_exposure / spot_price
                current_position = {
                    'ticker': stock_ticker,
                    'Option Open Date': date,
                    'Spot Price': spot_price,
                    'Option Strike': option_target_delta['Strike Price'],
                    'Option Initial Price': option_initial_price,
                    'Option Final Price': 0,
                    'Option Close Date': 0,
                    'Options PNL': 0,
                    'lot_size': lot_size,
                    'Options SL': 0,
                    'Re-entry': False,
                    'Reentry Count': reentry_count
                }
                option_open = True
                is_position_open = True
                re_entry_open = False  # No re-entry yet
                reentry_count = 0  # Reset the re-entry count
                is_expiry = False
                #print(f"Eentering call option for {stock_ticker} on {date} at price {option_initial_price}")
                continue

            if is_position_open:
                option_price_close = get_option_price(options_for_date, current_position['Option Strike'], option_type, 'High')
                option_price_open = get_option_price(options_for_date, current_position['Option Strike'], option_type, 'Open')
                if option_open:
                    if option_price_open >= (1 + sl) * current_position['Option Initial Price']:  # SL hit overnight
                        option_exit_price = option_price_open
                        current_position['Options PNL'] = (current_position['Option Initial Price'] - option_exit_price) * current_position['lot_size']  # Selling 2 lots
                        current_position['Option Close Date'] = date
                        current_position['Option Final Price'] = option_exit_price
                        current_position['Option SL'] = 'Overnight SL Hit'
                        option_open = False
                        re_entry_open = True  # Enable re-entry
                        sl_position = copy.deepcopy(current_position)
                        option_trades.append(sl_position)  # Store the original position
                        #print("Overnight SL got hit on " + date)

                    elif option_price_close >= (1 + sl) * current_position['Option Initial Price']:  # SL hit intraday
                        option_exit_price = (1 + sl) * current_position['Option Initial Price']
                        current_position['Options PNL'] = (current_position['Option Initial Price'] - option_exit_price) * current_position['lot_size']
                        current_position['Option Close Date'] = date
                        current_position['Option Final Price'] = option_exit_price
                        current_position['Option SL'] = 'Intraday SL Hit'
                        option_open = False
                        re_entry_open = True  # Enable re-entry
                        sl_position = copy.deepcopy(current_position)
                        option_trades.append(sl_position)  # Store the original position
                        #print("Intraday SL got hit on " + date)


                # Re-entry logic: Check if price goes below original entry price after SL hit, and re-entry count is within the limit
                if re_entry_open and option_open is False and reentry_count < max_reentries:
                    if reentry_type == "cost":
                        option_price_close = get_option_price(options_for_date, current_position['Option Strike'], option_type, 'Close')
                        if option_price_close <= option_entry_price:
                            current_position['Re-entry'] = True
                            current_position['Reentry Count'] = reentry_count
                            current_position['Option Open Date'] = date
                            current_position['Option Initial Price'] = option_price_close
                            current_position['Reentry Count'] = reentry_count + 1
                            option_open = True
                            reentry_count += 1  # Increment the re-entry count
                            re_entry_open = False  # Reset re-entry flag
                    elif reentry_type == "asap":
                        option_target_delta = find_option_by_delta(options_for_date, date, spot_price, time_to_maturity, volatility, target_delta, option_type)
                        option_initial_price = option_target_delta['Close']
                        option_entry_price = option_initial_price  # Store the original entry price for re-entry
                        current_position['Re-entry'] = True
                        current_position['Reentry Count'] = reentry_count
                        current_position['Option Open Date'] = date
                        current_position['Option Strike'] = option_target_delta['Strike Price']
                        current_position['Option Initial Price'] = option_initial_price
                        current_position['Reentry Count'] = reentry_count + 1
                        option_open = True
                        reentry_count += 1  # Increment the re-entry count
                        re_entry_open = False  # Reset re-entry flag

                # End of the month - Close all open positions
                if not is_expiry and calculate_time_to_expiry(date) == 1:
                    next_day = pd.to_datetime(date) + timedelta(days=1)
                    options_for_nextdate = options_data[options_data['Date'] == next_day.strftime('%Y-%m-%d')]
                    if options_for_nextdate.empty:
                        is_expiry = True

                if not is_expiry and calculate_time_to_expiry(date) == 0:
                    is_expiry = True

                if is_expiry:
                    if option_open:
                        #try:
                        option_exit_price = get_option_price(options_for_date, current_position['Option Strike'], option_type, 'Close')
                        #except:
                            #option_exit_price = 0
                        current_position['Options PNL'] = (current_position['Option Initial Price'] - option_exit_price) * current_position['lot_size']
                        current_position['Option Close Date'] = date
                        current_position['Option Final Price'] = option_exit_price
                        option_open = False
                        current_position['Option SL'] = 'No SL Hit'
                        option_trades.append(current_position)  # Store the original position
                        #print("Closing position at expiry on " + date)
                    is_position_open = False
                    current_position = {}  # Reset position
        except:
            pass

    return option_trades