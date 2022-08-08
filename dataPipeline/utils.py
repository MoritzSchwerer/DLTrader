#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
import datetime
from datetime import date
import requests

key = os.environ['TIINGO_API_KEY']

def get_last_historic_date(last_date = date.today(), historical_days = 1450):
    '''
    Takes one date and range of days as an input
    As output come 2 variables:        the min/max dates as str value
    '''
    todays_year = last_date.year
    todays_month = last_date.month
    todays_day = last_date.day
    historical_date = last_date-datetime.timedelta(days=historical_days)
    historical_year = historical_date.year
    historical_month = historical_date.month
    historical_day = historical_date.day
    #Transfrom dates to str
    historical_date_str =str(historical_year) + "-" + str(historical_month) + "-" + str(historical_day)
    latest_date_str = str(todays_year) + "-" + str(todays_month) + "-" + str(todays_day)

    return historical_date_str, latest_date_str

def fetch_stock(symbol, last_date = date.today(), historical_days = 1450):
    '''
    Get the trading information about a stock for a range of days in "historical_days" before the "last_date"
    The output is a DataFrame with columns "close","high","low","open","volume","splitFactor"
    The output are adjusted prices
    '''
    #Get latest and historical day, month, year for API request
    historical_date_str, latest_date_str = get_last_historic_date(last_date, historical_days)

    #The request itself
    url = f'https://api.tiingo.com/tiingo/daily/{symbol}/prices?startDate={historical_date_str}&endDate={latest_date_str} '
    headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Token {key}'
            }
    r = requests.get(url, headers=headers)
    response = r.json()
    response = pd.DataFrame(response)
    response.set_index(["date"], inplace = True)
    response.index = pd.to_datetime(response.index)
    response.drop(columns = ["close","high","low","open","volume","splitFactor"],axis = 1, inplace = True)
    response.rename(columns = {"adjClose":"close","adjHigh":"high","adjLow":"low","adjOpen":"open","adjVolume":"volume"}, inplace = True)
    return response

def fetch_fundamentals(symbol, last_date = date.today(), historical_days = 1450):
    '''
    The output is a dataframe with daily fundamentals:
    Market_Cap, Enterprise_Value, PE_Ratio, PB_Ratio, Trailing PEG
    '''
    #Get latest and historical day, month, year for API request
    historical_date_str, latest_date_str = get_last_historic_date(last_date, historical_days)

    url = f'https://api.tiingo.com/tiingo/fundamentals/{symbol}/daily?token={key}?startDate={historical_date_str}&endDate={latest_date_str}'
    headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Token {key}'
            }
    r = requests.get(url, headers=headers)
    response = r.json()
    response = pd.DataFrame(response)
    response.set_index(["date"], inplace = True)
    response.index = pd.to_datetime(response.index)
    return response

def fetch_definitions():
    '''
    The output is the Dataframe with the definitions of parameters
    from the fetch_statements function

    '''
    url = f'https://api.tiingo.com/tiingo/fundamentals/definitions?token={key}'
    headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Token {key}'
            }
    r = requests.get(url, headers=headers)
    response = r.json()
    response = pd.DataFrame(response)
    response.to_csv("Definitions.csv")
    return response

def fetch_statements(symbol, last_date = date.today(), historical_days = 1450):
    '''
    Gets historical financial data about the stock like Total Assets, Accounts Payable, Short & Long term Debts etc.
    '''
    #Get latest and historical day, month, year for API request
    historical_date_str, latest_date_str = get_last_historic_date(last_date, historical_days)

    url = f'https://api.tiingo.com/tiingo/fundamentals/{symbol}/statements?token={key}?startDate={historical_date_str}&endDate={latest_date_str}'
    headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Token {key}'
            }
    r = requests.get(url, headers=headers)
    response = r.json()
    final_df = pd.DataFrame()
    for i in range(len(response)):
        balanceSheet_df = pd.DataFrame(response[i]["statementData"]["balanceSheet"]).T
        overview_df = pd.DataFrame(response[i]["statementData"]["overview"]).T
        cashFlow_df = pd.DataFrame(response[i]["statementData"]["cashFlow"]).T
        incomeStatement_df = pd.DataFrame(response[i]["statementData"]["incomeStatement"]).T
        dfs = [balanceSheet_df, overview_df, cashFlow_df, incomeStatement_df]
        general_df = pd.concat(dfs, axis =1)

        #Make 1st row a header
        new_header = list(general_df.loc["dataCode"]) #grab the first row for the header
        general_df.drop("dataCode",axis = 0, inplace = True) #take the data less the header row
        general_df.columns = new_header

        general_df["date"] = response[i]["date"]
        general_df["quarter"] = response[i]["quarter"]
        general_df["year"] = response[i]["year"]
        print(f'Combining Statements for {response[i]["year"]} and {response[i]["quarter"]}')
        final_df = pd.concat([final_df, general_df], axis = 0)
    final_df.set_index(["date"], inplace = True)
    #converting date from str to datetime object
    final_df.index = pd.to_datetime(final_df.index, yearfirst = True, utc = True, origin = "unix")
    final_df_copy = final_df.copy()
    #Dealing with duplicated indexes of datetime
    if final_df.index.duplicated().sum()>0:
        duplicates_in_df = final_df.index.duplicated()
        final_df.dropna(axis = 0, inplace = True)
    final_df = final_df.apply(pd.to_numeric, errors='ignore')
    return final_df

def fetch_metadata():
    '''
    Get's metadata about all available stocks, like:
        Industry, Location, Full Name, Ticker, etc.
    '''
    url = f'https://api.tiingo.com/tiingo/fundamentals/meta?token={key}'
    headers = {
            'Content-Type': 'application/json',
            'Authorization' : f'Token {key}'
            }
    r = requests.get(url, headers=headers)
    response = r.json()
    response = pd.DataFrame(response)
    return response

def combine_tables(stock_df, statemets_data, fundamentals_data, dates_range, metadata, todays_date = date.today()):
    '''
    Combining into one Dataframe information about daily trading values, statements, fundamentals
    '''
    #We create an index of dates range
    dates = pd.date_range(todays_date-datetime.timedelta(days=dates_range),todays_date,freq='d')
    #We change the name to make join with other dataframes available by the same index
    dates.rename('date', inplace = True)
    #Convert index to dataframe
    dates_df = pd.DataFrame(index = dates)
    #Delete timezone from the statements to make join available
    statemets_data.index = statemets_data.index.tz_convert(None)
    stock_df.index = stock_df.index.tz_convert(None)
    fundamentals_data.index = fundamentals_data.index.tz_convert(None)
    #Joining two dataframes
    dates_and_statemts = dates_df.join(statemets_data, how = 'left')
    #Statements table contains data about quarters only while dates DF is much bigger. We populate quarter results to days
    dates_and_statemts.ffill(axis = 0, inplace = True)
    stock_df = stock_df.join(dates_and_statemts, how = 'left')
    stock_df = stock_df.join(fundamentals_data, how = 'left')
    stock_df[["sector","industry"]] = metadata[["sector","industry"]].iloc[-1]
    return stock_df

def get_combined_data(symbol, historical_dates_range):
    todays_date = date.today()

    #Get all companies sector, industry, location, etc
    metadata = fetch_metadata()

    #Hitorical Prices Open, Close, Low, High, Volume for historical data
    stock_dataset = fetch_stock(symbol, todays_date, historical_dates_range)
    # Getting stock market cap, PE Ratio, PB Ratio
    fundamentals = fetch_fundamentals(symbol, todays_date, historical_dates_range)
    # Financial KPIs
    statements = fetch_statements(symbol, todays_date, historical_dates_range)
    # Get stock sector and industry
    stock_metadata = metadata[metadata.ticker == symbol.lower()][["sector","industry"]].copy()

    # Combining all stock's data

    big_dataset = combine_tables(stock_dataset, statements, fundamentals, historical_dates_range, stock_metadata)

    big_dataset.sort_values(by = 'date', axis = 0, ascending = False, inplace = True)
    big_dataset["Debt-to-Equity_Ratio"] = big_dataset["totalAssets"]/big_dataset["totalLiabilities"]
    big_dataset["DividendsYield"] = big_dataset["payDiv"]/big_dataset["marketCap"]
    big_dataset["PayoutRatio"] = big_dataset["payDiv"]/big_dataset["grossProfit"]
    big_dataset["Acc_Rec_Pay_Ration"] = big_dataset["acctRec"]/big_dataset["acctPay"]
    big_dataset["Earnings_per_stock"] = big_dataset["epsDil"]/big_dataset["close"]

    return big_dataset
