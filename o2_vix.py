import os
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.regression as lm
from datetime import timedelta, datetime as dt

plt.style.use('ggplot')

BASE_URL_FUT = "Data/vix_futures/data/vix_futures/Tickdata_VX_Quote/"
BASE_URL_IND = "Data/vix_futures/data/vix_futures/IAP/IAP.csv"
BASE_URL_IND_FUT = "Data/vix_futures/S&P 500 VIX Futures Historical Data.csv"
FUT_MAP = {1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M', 7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z'}

# Data description:

# For VIX Index, the data source is IAP.csv is a time series of VIX in CDT at 15 seconds intervals.
# VIX index disseminated from 2:15am CDT to 3:15pm CDT with a 15min break between 8:15am to 8:30am

# For VIX Futures the data is in ET. 

# For S&P 500 VIX Futures Historical Data.csv, each entry shows the prices of the VIX Future expiring 
# that month of the entry date


def filter_futures_data(contract, event_dt, period_range, frequency=None):
    """
    Given an indicated datetime of a macro event, a specified look through range of datetime interval, and a specified contract
    Return all data of the specified contract that falls in the in the interval as a pandas dataframe

    If the speicified contract does not contain any data that falls in the specified datetime of the macro event and the 
    specified looking range period, return an empty dataframe

    Parameters
    ----------
    contract : str 
        [M][YY]
        M = Contract month
            F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
        YY = Contract year
    event_dt : datetime.datetime
        The datetime of event
    period_range : datetime.timedelta
        An absolute time difference
    frequency: str
        A string indicating the fixed frequency at which we are sampling the data. If none, take the base 5 minutes frequency
            Offical frequency alias https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects

    """
    # Both "Date" and "Time" columns are strings
    contract_data = BASE_URL_FUT + "/VX" + contract + ".csv"
    df = pd.read_csv(contract_data)
    start_dt = event_dt - period_range
    end_dt = event_dt + period_range
    df["EndDt"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    mask = (df['EndDt'] >= start_dt) & (df['EndDt'] <= end_dt)
    df = df.loc[mask]
    if frequency:
        # TODO: dt.floor() only rounds for fixed frequency
        df = df[df['EndDt'] == df['EndDt'].dt.floor(frequency)]
        
    return df


def filter_events(contract, events):
    """
    Given a contract, and a list of events(timestamp) 
    Return a sub-list of events(timestamps) that occurs in the lifespan of the contract

    Parameters
    ----------
    contract : str
        [M][YY]
        M = Contract month
            F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
        YY = Contract year
    events : list like objects containing datetime objects
        An array containing dates of events 
    """
    contract_data = BASE_URL_FUT + "/VX" + contract + ".csv"
    df = pd.read_csv(contract_data)
    df["EndDt"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    start_dt = df['EndDt'].iloc[0]
    end_dt = df['EndDt'].iloc[-1]
    mask = (events >= start_dt) & (events <= end_dt)
    return events.loc[mask]


def plt_contract(contract, events, frequency=None):
    """
    Given a contract and a frequency, plot the contract's full time series of its' mid-point of bid-ask,
    marking the timestamps of when events occurs.

    Parameters
    ----------
    contract : str 
        [M][YY]
        M = Contract month
            F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
        YY = Contract year
    """
    contract_data = BASE_URL_FUT + "/VX" + contract + ".csv"
    df = pd.read_csv(contract_data)
    df["EndDt"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    if frequency:
        # TODO: dt.floor() only rounds for fixed frequency
        df = df[df['EndDt'] == df['EndDt'].dt.floor(frequency)]
    filt_events = filter_events(contract, events)

    fig, ax = plt.subplots(figsize=(12,8))
    handles, labels = ax.get_legend_handles_labels()

    midp = 0.5*(df['Close Bid Price'] + df['Close Ask Price'])
    ax.plot(df['EndDt'], midp, color='b')
    ax.set_title(f'VX{contract}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Timestamp', fontsize=12)
    ax.set_ylabel(f'Mid-point of bid-ask price of VX{contract}', fontsize=12)
    for event_dt in filt_events:
        event_line = ax.axvline(x=event_dt, color='red', linestyle='--', label=f'{event_dt} FOMC')
        handles.append(event_line)
        labels.append(f'FOMC {event_dt}')
    ax.legend(handles, labels)

    plt.show()


def plt_contract_event(contract, event_dt=None, period_range=None, frequency=None):
    """
    Given an indicated datetime of a macro event, a specified range of datetime interval, a specified contract, 
    and a frequency of contract data
    Plot the specified contract data that falls in the in the interval at the specified frequency

    Parameters
    ----------
    contract : str 
        [M][YY]
        M = Contract month
            F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
        YY = Contract year
    event_dt : datetime.datetime
    period_range : datetime.timedelta
    frequency: str
    """
    raw_df = filter_futures_data(contract, event_dt=event_dt, period_range=period_range, frequency=frequency)
    
    fig, ax = plt.subplots(figsize=(12,8))
    handles, labels = ax.get_legend_handles_labels()

    midp = 0.5*(raw_df['Close Bid Price'] + raw_df['Close Ask Price'])
    ax.plot(raw_df['EndDt'], midp, color='b')
    ax.set_title(f'VX{contract}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Timestamp ', fontsize=12)
    ax.set_ylabel(f'Mid-point of bid-ask price of VX{contract}', fontsize=12)
    event_line = ax.axvline(x=event_dt, color='red', linestyle='--', label=f'{event_dt} FOMC')
    handles.append(event_line)
    labels.append(f'{event_dt} FOMC')
    ax.legend(handles, labels)
    plt.show()


def plt_event_dist(contract, event_dt=None, period_range=None, frequency=None):
    """
    Given an indicated datetime of a macro event, a specified range of datetime interval, a specified contract, 
    and a frequency of contract data
    Plot the distribution of the specified contract data before and after the event dt

    Parameters
    ----------
    contract : str 
        [M][YY]
        M = Contract month
            F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
        YY = Contract year
    event_dt : datetime.datetime
    period_range : datetime.timedelta
    frequency: str
    """
    raw_df = filter_futures_data(contract, event_dt=event_dt, period_range=period_range, frequency=frequency)
    raw_df['MidPt'] = 0.5*(raw_df['Close Bid Price'] + raw_df['Close Ask Price'])

    bef_df = raw_df[raw_df['EndDt'] < event_dt]
    aft_df = raw_df[raw_df['EndDt'] > event_dt]
    
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(12,8))
    
    for ax, column, color, title in zip(axes.ravel(), [bef_df['MidPt'], aft_df['MidPt']], ['teal', 'orange'], ['Before', 'After']):
        ax.hist(column, color=color,)
        ax.set_title(title + ' ' + f'FOMC {event_dt}')
        ax.set_ylabel('Count')
        ax.set_xlabel('Mid-point of bid-ask')
        ax.axvline(x=np.mean(column), color='red', linestyle='--', label='Mean')
        ax.legend()
        
    plt.show()


# TODO: Construct VIX Premiums using VIX futures prices and VIX Tick data
def get_vix_premium(t):
    """
    Given a specified timestamp t, return the rolling 1 month VIX premium at time t as termed in Prof Cheng's paper.

    Implementation: 

    As stated the in the paper, assuming no arbitrage, the risk neutral expecation of VIX at time t is
    estimated with the price of the one month ahead expiration future . The physical expectation is estimated 
    with an ARMA(2,2) model on the VIX time series using data from pre-2004.

    We then take their differences adjusted by the number of trading days (minus expiration day) to get a daily 
    rolling 1 month VIX premium

    The paper sugguests that using a rolling ARMA forecast model that uses information available up to but 
    excluding date t should improve forecast estimates

    Adjustments to the implementation:
    - Calculate the 1 month VIX premium at a shorter frequency (If there are data on shorter frequency expiration 
    contracts, then a shorter VIX premium could be calculated at a desired frequency)

    Parameters:
    ----------
    t: datetime
        timestamp to calculate the VIX premium
    F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
    """
    contract_m = FUT_MAP[t.month + 1]
    contract_y = str(t.year)[-2:]
    contract_data = BASE_URL_FUT + "/VX" + contract_m + contract_y + ".csv"
    contract_df = pd.read_csv(contract_data)
    contract_df["EndDt"] = contract_df.to_datetime(contract_df["Date"] + " " + contract_df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    pass

# TODO: 
#       Compare VIX Index, VIX chained futures prices at a daily frequency 10 days out and 10 days after FOMC annoucments.

#       Graphical: Plot both series with respect to the FOMC annoucments, including the lookthrough period range
#                  Plot the daily changes with respect to the FOMC annoncments

#       Numeric: For each t=-10, -9, ..., 0, 1, 2, ..., 10, calculate the mean change in daily changes


def get_rolling_contract(event_dt, roll=1):
    """
    Return the dataframe of the rolling contract defined by the event_dt and roll.

    Parameters
    ----------
    event_dt : datetime.datetime or list of datetime.datetime
        The datetime of events
    roll : int
        The number of forward rolling month
    """
    month = event_dt.month 
    year = event_dt.year
    if month == 12: # special case for December
        month = 1
        year += 1 
    else: 
        month += 1
    month_str = FUT_MAP[month]
    year_str = str(year)[-2:]
    url = BASE_URL_FUT + "VX" + month_str + year_str + ".csv"
    return pd.read_csv(url)
    
print(get_rolling_contract(dt(2014, 12, 13)))

def get_range_d(df, event_dt, range_width):
    """
    Return a multi-indexed dataframe containing entries satisfying the number given by 
    the range_width with respect to event_dt

    Parameters
    ----------
    df: DataFrame
        The dataframe to filter
    event_dt : datetime.datetime or list of datetime.datetime
        The datetime of events
    range_width : int
        Number of entries to add before and after event_dt
    """
    if type(event_dt) is list:
        result = pd.DataFrame()
        for date in event_dt: 
            if date not in df.index: # if an event datetime is not the dataframe
                continue 
            event_row = df.index.get_loc(date)
            start = event_row - range_width
            end = event_row + range_width 
            filt = df.iloc[start:end + 1, :].reset_index()
            filt['Event Date'] = date
            filt['Date Label'] = [-i for i in range(range_width, 0, -1)] + [0] + \
                                 [i for i in range(1, range_width+1)]
            filt = filt.set_index(['Event Date', 'Date'])
            result = pd.concat([result, filt])
    else:
        event_row = df.index.get_loc(event_dt)
        start = event_row - range_width
        end = event_row + range_width 
        result = df.iloc[start:end + 1, :].reset_index()
        result['Event Date'] = event_dt
        result['Date Label'] = [-i for i in range(range_width, 0, -1)] + [0] + \
                                [i for i in range(1, range_width+1)]
        result = result.set_index(['Event Date', 'Date'])
    return result




def plot_ts_vix_vixfut(df_1, df_2, events, q):
    """
    Plot the timeseries of quantity q of vix and vix futures, marking the days of the events
    """
    fig, ax = plt.subplots(figsize=(14,10))
    handles, labels = ax.get_legend_handles_labels()
    ax.plot(df_1['Date'], df_1[q],color='b', label='VIX')
    ax.plot(df_2['Date'], df_2[q],color='g', label='VIX Futures')
    ax.set_xlabel('Timestamp', fontsize=12)
    
    for event_dt in events:
        event_line = ax.axvline(x=event_dt, color='red', linestyle='--', label=f'{event_dt} FOMC')
        handles.append(event_line)
        labels.append(f'FOMC {event_dt}')

    ax.legend(handles, labels)
    plt.show()









