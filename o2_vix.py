# How does Vix Futures and VIX premiums behave around macro announcements(FOMC) at the daily and intra-day frequency?
# Eg: Intra day Vix Futures bevaiour around FOMC announcement time 
import os
import pandas as pd 
from datetime import timedelta, datetime as dt

BASE_URL = "Data/vix_futures/data/vix_futures/Tickdata_VX_Quote"


# print(os.getcwd())


def filter_futures(contract, event_dt=None, period_range=None, frequency=None):
    """
    Given an indicated datetime of a macro event, a specified look through range of datetime interval, 
    a specified frequency and a specified contract
    Return all data of the specified contract that falls in the in the interval as a pandas dataframe

    If the speicified contract does not contain any data that falls in the specified datetime of the macro event and the 
    specified looking range period, return an empty dataframe

    Parameters
    ----------
    annoucement_dt : datetime.datetime
    period_range : datetime.timedelta
        An absolute time difference
    contract : str 
        [M][YY]
        M = Contract month (see table below)
        YY = Contract year

        *	F	Jan;
        *	G	Feb;
        *	H	Mar;
        *	J	Apr;
        *	K	May;
        *	M	Jun;
        *	N	Jul;
        *	Q	Aug;
        *	U	Sep;
        *	V	Oct;
        *	X	Nov;
        *	Z	Dec;
    """
    # Both "Date" and "Time" columns are strings
    contract_data = BASE_URL + "/VX" + contract + ".csv"
    df = pd.read_csv(contract_data)
    start_dt = event_dt - period_range
    end_dt = event_dt + period_range
    print(start_dt)
    print(end_dt)
    df["EndDt"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    mask = (df['EndDt'] >= start_dt) & (df['EndDt'] <= end_dt)
    return df.loc[mask]


# Test
date = dt(2012, 8, 15, 14, 0, 0)
range_ = timedelta(days=1, hours=12, seconds=0)
print(filter_futures("F13", event_dt=date, period_range=range_))






