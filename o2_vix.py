# How does Vix Futures and VIX premiums behave around macro announcements(FOMC) at the daily and intra-day frequency?
# Eg: Intra day Vix Futures behaviour around FOMC announcement time 
import os
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta, datetime as dt

BASE_URL = "Data/vix_futures/data/vix_futures/Tickdata_VX_Quote"
plt.style.use('ggplot')


# print(os.getcwd())


def filter_futures(contract, event_dt, period_range, frequency=None):
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
        A string indicating the frequency at which we are sampling the data. If none, take the base 5 minutes frequency
            Offical frequency alias https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases

    """
    # Both "Date" and "Time" columns are strings
    contract_data = BASE_URL + "/VX" + contract + ".csv"
    df = pd.read_csv(contract_data)
    start_dt = event_dt - period_range
    end_dt = event_dt + period_range
    df["EndDt"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    mask = (df['EndDt'] >= start_dt) & (df['EndDt'] <= end_dt)
    df = df.loc[mask]
    if frequency:
        df = df[df['EndDt'] == df['EndDt'].dt.floor(frequency)]
    return df


def filter_event(contract, events):
    """
    Given a contract, return the date time of events that it experiences in the lifespan of the contract

    Parameters
    ----------
    contract : str
        [M][YY]
        M = Contract month
            F=Jan; G=Feb; H=Mar; J=Apr; K=May; M=Jun; N=Jul; Q=Aug; U=Sep; V=Oct; X=Nov; Z=Dec;
        YY = Contract year
    events : list like objected
        An array containing dates of events 
    """
    contract_data = BASE_URL + "/VX" + contract + ".csv"
    df = pd.read_csv(contract_data)
    df["EndDt"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
    start_dt = df['Date'][0]
    end_dt = df['Date'][-1]
    mask = (events >= start_dt) & (events <= end_dt)
    return events.loc[mask]


def plt_event(contract, event_dt=None, period_range=None, frequency=None):
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
    raw_df = filter_futures(contract, event_dt=event_dt, period_range=period_range, frequency=frequency)
    
    fig, ax = plt.subplots(figsize=(10,6))
    handles, labels = ax.get_legend_handles_labels()

    midp = 0.5*(raw_df['Close Bid Price'] + raw_df['Close Ask Price'])
    ax.plot(raw_df['EndDt'], midp, color='b')
    ax.set_title(f'VX{contract}', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date Time', fontsize=12)
    ax.set_ylabel(f'Mid point of bid and ask price of VX{contract}', fontsize=12)
    event_line = ax.axvline(x=event_dt, color='red', linestyle='--', label='FOMC Announcment')
    handles.append(event_line)
    labels.append('FOMC Announcment')
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
    raw_df = filter_futures(contract, event_dt=event_dt, period_range=period_range, frequency=frequency)
    raw_df['MidPt'] = 0.5*(raw_df['Close Bid Price'] + raw_df['Close Ask Price'])

    bef_df = raw_df[raw_df['EndDt'] < event_dt]
    aft_df = raw_df[raw_df['EndDt'] > event_dt]
    
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(10,6))
    
    for ax, column, color in zip(axes.ravel(), [bef_df['MidPt'], aft_df['MidPt']], ['teal', 'orange']):
        ax.hist(column, color=color)
        ax.axvline(x=np.mean(column), color='red', linestyle='--', label='Mean')
        ax.legend()
        
    plt.show()
    


# Example usage
if __name__ == "__main__":
    find_contracts(dt(2019, 9, 18, 14, 0, 0))
    # date = dt(2019, 9, 18, 14, 0, 0)
    # range_ = timedelta(days=0, hours=12, seconds=0)
    # frequency = 's'
    # plt_event_dist("Z19", event_dt=date, period_range=range_)





