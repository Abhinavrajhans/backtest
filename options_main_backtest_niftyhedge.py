# options_main_backtest.py

import pandas as pd
import multiprocessing as mp
from datetime import datetime
from options_backtest_nifty import backtest_options  # Importing from the options backtest module
nifty_options_data=pd.read_pickle('Nifty_MonthlyI_Opt2019.pkl')
nifty_index_data=pd.read_csv('nifty_combined_sorted_data.csv')

# Define tickers
tickers = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "HINDUNILVR", "KOTAKBANK", "SBIN",
           "BHARTIARTL", "ITC", "ASIANPAINT", "BAJFINANCE", "MARUTI", "AXISBANK", "LT", "HCLTECH",
           "SUNPHARMA", "WIPRO", "ULTRACEMCO", "TITAN", "TECHM", "NESTLEIND", "JSWSTEEL", "TATASTEEL",
           "POWERGRID", "ONGC", "COALINDIA", "INDUSINDBK", "BAJAJFINSV", "GRASIM", "CIPLA", "ADANIPORTS",
           "TATAMOTORS", "DRREDDY", "BRITANNIA", "HEROMOTOCO", "DIVISLAB", "EICHERMOT", "SHREECEM",
           "APOLLOHOSP", "UPL", "TATACONSUM", "BAJAJ_AUTO", "HINDALCO", "SBILIFE", "VEDL"]

# Strategy Parameters
sl = 2  # Stop loss percentage
max_reentries = 0  # Limit the number of re-entries
dte = 20
target_delta = 0.35
option_type = "call"
mode = "sell"
mode_nifty = "buy"
reentry_type = "asap"

import warnings
# Suppress only SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)

# Create a function to run the options backtest for each stock
def run_options_backtest(ticker):
    equity_data = pd.read_csv(f'Stocks_Data/{ticker}_EQ_EOD.csv')
    options_data = pd.read_csv(f'Stocks_Data/{ticker}_Opt_EOD.csv')

    # Set the start and end dates for backtesting
    start_date_eq = equity_data['Date'].iloc[0]
    start_date = (datetime.strptime(start_date_eq, "%Y-%m-%d") + pd.DateOffset(months=0)).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + pd.DateOffset(months=66)).strftime("%Y-%m-%d")

    stock_ticker = f'{ticker}.EQ-NSE'
    total_exposure = 700000

    # Run the options backtest
    try:
        options_trades = backtest_options(stock_ticker, equity_data, options_data,nifty_options_data,nifty_index_data, start_date, end_date, total_exposure, dte, sl, target_delta = target_delta, max_reentries=max_reentries, reentry_type = reentry_type, option_type=option_type)
        options_trades_df = pd.DataFrame(options_trades)

        # Calculate final PNL
        final_options_pnl = options_trades_df['Options PNL'].sum() + options_trades_df['Nifty Options PNL'].sum()

        print(f"Options Backtest for {ticker}: PNL: {final_options_pnl}")

        return {
            "ticker": ticker,
            "Options PNL": final_options_pnl,
            "Options Tradebook": options_trades_df
        }
    except:
        return {
            "ticker": ticker,
            "Options PNL": 0,
            "Options Tradebook": pd.DataFrame()
        }

# Function to calculate Max Drawdown
def calculate_max_drawdown(cumulative_pnl):
    drawdowns = (cumulative_pnl - cumulative_pnl.cummax())
    max_drawdown = drawdowns.min()
    return max_drawdown

# Use multiprocessing to run the options backtest in parallel
if __name__ == "__main__":
    pool = mp.Pool(mp.cpu_count())  # Use all available CPU cores

    # Map the tickers to the run_options_backtest function
    results = pool.map(run_options_backtest, tickers)

    # Close the pool to free up resources
    pool.close()
    pool.join()

    # Process the results after options backtesting
    options_tradebook_df = pd.DataFrame()

    for result in results:
        options_tradebook_df = pd.concat([options_tradebook_df, result['Options Tradebook']], ignore_index=True)

    # Save the options tradebook to CSV
    options_tradebook_df.to_csv("Tradebooks/Options_Tradebook_"+option_type+"_" + str(sl)+"_"+str(dte)+str(target_delta)+"_BUY_NiftyHedge"+".csv", index=False)

    if mode == "buy":
        options_tradebook_df['Options PNL'] = options_tradebook_df['Options PNL']*-1 
    if mode_nifty == "buy":
        print("Nifty Options Buy")
        options_tradebook_df['Nifty Options PNL'] = options_tradebook_df['Nifty Options PNL']*-1

    # Calculate and print the total PNL for all stocks
    total_options_pnl = options_tradebook_df['Options PNL'].sum() + options_tradebook_df['Nifty Options PNL'].sum()
    print(f"\nTotal Combined Options PNL for all stocks: {total_options_pnl:.2f}")

    # Add 'Year' and 'Month' columns to calculate yearly and monthly PNL
    options_tradebook_df['Option Open Date'] = pd.to_datetime(options_tradebook_df['Option Open Date'])
    options_tradebook_df['Year'] = options_tradebook_df['Option Open Date'].dt.year
    options_tradebook_df['Month'] = options_tradebook_df['Option Open Date'].dt.to_period('M')

    # Group by 'Year' and sum the PNL for yearly PNL
    yearly_pnl_df = options_tradebook_df.groupby('Year').agg({
        'Options PNL': 'sum',
        'Nifty Options PNL': 'sum'
    }).reset_index()

    # Group by 'Month' and sum the PNL for monthly PNL
    monthly_pnl_df = options_tradebook_df.groupby('Month').agg({
        'Options PNL': 'sum',
        'Nifty Options PNL': 'sum'
    }).reset_index()
    monthly_pnl_df.to_csv("Call_Sell_Monthly_PNL"+str(dte)+" "+str(target_delta)+"_BUY_NiftyHedge"+".csv")
    # Calculate cumulative P&L for max drawdown
    cum_pnl = monthly_pnl_df['Options PNL'].cumsum() + monthly_pnl_df['Nifty Options PNL'].cumsum()

    # Calculate Max Drawdown
    max_drawdown = calculate_max_drawdown(cum_pnl)
    print(f"\nMax Drawdown: {max_drawdown:.2f}")

    # Calculate Average Yearly Returns
    average_yearly_returns = (yearly_pnl_df['Options PNL'].mean() + yearly_pnl_df['Nifty Options PNL'].mean()) / 2
    print(f"\nAverage Yearly Returns: {average_yearly_returns:.2f} Rupees")

    # Calculate Average Monthly Returns
    average_monthly_returns = (monthly_pnl_df['Options PNL'].mean() + monthly_pnl_df['Nifty Options PNL'].mean()) / 2
    print(f"\nAverage Monthly Returns: {average_monthly_returns:.2f} Rupees")

    # Calculate Max Loss in a Month
    max_loss_month = min(monthly_pnl_df['Nifty Options PNL'].min(), monthly_pnl_df['Options PNL'].min())
    print(f"\nMax Loss in a Month: {max_loss_month:.2f} Rupees")

    # Calculate Max Loss in a Month
    max_profit_month = max(monthly_pnl_df['Nifty Options PNL'].max(),monthly_pnl_df['Options PNL'].max())
    print(f"\nMax Profit in a Month: {max_profit_month:.2f} Rupees")

    # Print yearly PNL summary in raw rupee numbers
    print("\nYearly PNL Summary (in Rupees):")
    print(yearly_pnl_df)

    # Calculate overall totals for all years
    total_options_pnl_by_year = yearly_pnl_df['Options PNL'].sum()
    total_nifty_options_pnl_by_year = yearly_pnl_df['Nifty Options PNL'].sum()
    print(f"\nTotal Options PNL (across all years): {total_options_pnl_by_year:.2f}")
    print(f"\nTotal Nifty Options PNL (across all years): {total_nifty_options_pnl_by_year:.2f}")
    # Calculate the number of positive and negative months
    positive_months = monthly_pnl_df[monthly_pnl_df['Options PNL'] > 0].shape[0]
    negative_months = monthly_pnl_df[monthly_pnl_df['Options PNL'] < 0].shape[0]

    positive_nifty_months = monthly_pnl_df[monthly_pnl_df['Nifty Options PNL'] > 0].shape[0]
    negative_nifty_months = monthly_pnl_df[monthly_pnl_df['Nifty Options PNL'] < 0].shape[0]

    # Print the results
    print(f"\nNumber of Positive Months: {positive_months}")
    print(f"Number of Negative Months: {negative_months}")
    print(f"Number of Positive Nifty Months: {positive_nifty_months}")  
    print(f"Number of Negative Nifty Months: {negative_nifty_months}")