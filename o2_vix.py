import pandas as pd 
import pyfedwatch as fw
import scipy
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import math
from random import randrange
from datetime import timedelta, datetime as dt
from pyfedwatch.datareader import read_price_history
from pyfedwatch.datareader import get_fomc_data
plt.style.use('ggplot')
palette = sns.color_palette("husl", 3)

# Data description:

# For VIX Index, the data source is IAP.csv is a time series of VIX in CDT at 15 seconds intervals.
# VIX index disseminated from 2:15am CDT to 3:15pm CDT with a 15min break between 8:15am to 8:30am

# For VIX Futures contract files the data is in ET. 

# For S&P 500 VIX Futures Historical Data.csv, each entry shows the prices of the VIX Future expiring 
# that month of the entry date

BASE_URL_FUT = "Data/vix_futures/data/vix_futures/Tickdata_VX_Quote/"
BASE_URL_IND = "Data/vix_futures/data/vix_futures/IAP/IAP.csv"
BASE_URL_IND_FUT = "Data/vix_futures/S&P 500 VIX Futures Historical Data.csv"
BASE_URL_DATES = "Data/macro/data/macro/macro_announcement_dates_202112.csv"
BASE_URL_FEDFUT = 'Data/ff_futures/contracts'
FOMC_DATA = get_fomc_data()
FOMC_DATES = FOMC_DATA[(FOMC_DATA['Status'] == 'Scheduled') | (FOMC_DATA['Status'] == 'Cancelled')].index.tolist()
FUT_MAP = {1:'F', 2:'G', 3:'H', 4:'J', 5:'K', 6:'M', 7:'N', 8:'Q', 9:'U', 10:'V', 11:'X', 12:'Z'}


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


def random_date(start, end, num):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    result = []
    while num > 0:
        delta = end - start
        int_delta = delta.days 
        random = randrange(int_delta)
        result.append(start + timedelta(random))
        num -= 1
    return result 


def forward_roll_contract(event_dt, roll=1):
    """
    Return data of forward rolling expiration contract defined by the event_dt and roll.

    Parameters
    ----------
    event_dt : datetime.datetime or list of datetime.datetime
        The datetime of events
    roll : int
        The number of forward rolling month
    """
    month = event_dt.month 
    year = event_dt.year
    if (month + roll) > 12 : # Roll to next year
        month = (month + roll) % 12
        year += 1 
    else: 
        month += roll
    month_str = FUT_MAP[month]
    year_str = str(year)[-2:]
    url = BASE_URL_FUT + "VX" + month_str + year_str + ".csv"
    return pd.read_csv(url)


def get_range_multi(event_dt, roll, range_width, freq):
    """
    Return a multi-indexed dataframe containing data of forward roll contract of event_dt, filtered as
    given by the range_width and freq 

    Parameters
    ----------
    event_dt : list of datetime.datetime
        The datetime of events
    roll:
        The number of forward rolling month for VIX Futures contract
    range_width : int
        Number of entries to add before and after event_dt
    freq : str
        The frequency to adjust the dataframe
    """
    result = pd.DataFrame()
    for dt in event_dt: 
        try: # if a contract for the date cannot be found
            df = forward_roll_contract(dt, roll)
        except Exception:
            continue 
        df["Datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S.%f")
        df = df.set_index('Datetime')
        df = df.resample(freq).first() 
        # Remove time not from 9-4
        df = df.between_time('9:00', '16:00')
        df.dropna(inplace=True)
        try: # If the rolling contract does not contain data for this event datetime. Eg: 2012/06/20
            event_row = df.index.get_loc(dt)
        except Exception:
            continue
        start = event_row - range_width - 1 
        end = event_row + range_width 
        filt = df.iloc[start:end + 1, :].reset_index()
        filt['Event Datetime'] = dt
        filt['Datetime Label'] = [-i for i in range(range_width + 1, 0, -1)] + [0] + \
                                [i for i in range(1, range_width+1)]
        filt = filt.set_index(['Event Datetime', 'Datetime'])
        result = pd.concat([result, filt])
    return result


def get_range(df, event_dt, bef, aft, freq=None):
    """
    Return a multi-indexed dataframe from df containing entries satisfying the number given by 
    the range_width with respect to event_dt and freq

    Parameters
    ----------
    df: DataFrame
        The dataframe to filter
    event_dt : list of datetime.datetime
        The datetime of events
    range_width : int
        Number of entries to add before and after event_dt
    freq : str
        The frequency to adjust the dataframe,
    """
    df = df.resample(freq).first()
    df = df.dropna()
    result = pd.DataFrame()
    for date in event_dt: 
        if date not in df.index: # if an event datetime is not the dataframe
            continue 
        event_row = df.index.get_loc(date)
        start = event_row - bef - 1
        end = event_row + aft 
        filt = df.iloc[start:end + 1, :].reset_index()
        # The look through period might not contain as much data as we want
        if filt.shape[0] == 0:
            continue
        filt.set_index('Datetime', inplace=True)
        event_row_filt = filt.index.get_loc(date)
        bef_filt = event_row_filt - 1
        aft_filt = filt.shape[0] - event_row_filt
        filt.reset_index(inplace=True)
        filt['Event Datetime'] = date
        filt['Datetime Label'] = [-i for i in range(bef_filt + 1, 0, -1)] + [0] + \
                                [i for i in range(1, aft_filt)]
        filt = filt.set_index(['Event Datetime', 'Datetime'])
        result = pd.concat([result, filt])
    return result


def get_cross_sec_avg(df, price=False, pct_change=True, plot=True, t_test=True, freq='d'):
    """
    Return a dataframe containing the cross sectional average for each of the intervals

    Parameters
    ----------
    df: DataFrame
        The dataframe of 
    price : Bool
        If True, calculate a price column
    pct_change : Bool
        If True, calculate a pct change column
    plot : Bool
        If True, plot the bar plot of the cross sectional averages
    t_test: Bool
        If True, print the intervals that are significantlly different from zero based on t-test
    """
    if price:
        df['Price'] = (df['Close Bid Price'] + df['Close Ask Price']) / 2
    if pct_change:
        df['Change %'] = df.groupby(level='Event Datetime')['Price'].pct_change() * 100
        df.dropna(inplace=True)
    df_ca = df.groupby('Datetime Label')['Change %'].mean().reset_index()
    if plot:
        fig, ax = plt.subplots(figsize=(8,5))
        ax.bar(df_ca['Datetime Label'], df_ca['Change %'],color='b')
        ax.set_xlabel(f'{freq} to/from Event', fontsize=12)
        ax.set_ylabel(f"% Change from previous {freq}", fontsize=12)
        plt.show()
    if t_test:
        for i in df_ca['Datetime Label']:
            result = scipy.stats.ttest_1samp(df[df['Datetime Label']==i]['Change %'],  0)
            if result.pvalue < 0.05:
                print(f"The Change % of T{i} is significant, degrees of freedom: {result.df}")
    return df_ca


def get_indiv_fomc(fomc_datetimes, vix, sp500, range, freq, roll):
    """ 
    Return a 2-dimensional array x where x[i] represent a list of dataframes 
    of cumilative changes / log return in VIX, VIX Futures and S&P500 for a single fomc event

    Parameters
    ----------
    fomc_datetimes: List like
        The list of fomc datetimes to calculate daily trend
    vix : Dataframe
        The dataframe of VIX to filter
    sp500: Dataframe
        The dataframe of SP500 to filter
    range : int
        Number of entries to add before and after the event datetime
    freq : str
        The frequency interval for the entries
    roll: int
        The number of forward rolling month for VIX Futures contract
    """
    result = []
    for datetime in fomc_datetimes:
        try:
            datetime = [datetime]
            vix_ = get_range(vix, datetime, range, range, freq)
            vix_['% Change'] = vix_.groupby('Event Datetime')['Price'].pct_change() * 100
            vix_.fillna(0, inplace=True)
            vix_['Cum. % Change'] = vix_.groupby('Event Datetime')['% Change'].cumsum()

            vix_fut = get_range_multi(datetime, roll, range, freq)
            vix_fut['Price'] = (vix_fut['Close Bid Price'] + vix_fut['Close Ask Price']) / 2
            vix_fut = vix_fut[['Symbol', 'Open Time', 'Close Time', 'Price', 'Datetime Label']]
            vix_fut['Log Price'] = np.log(vix_fut['Price'])
            vix_fut['Log Return %'] = vix_fut.groupby('Event Datetime')['Log Price'].diff() * 100
            vix_fut.fillna(0, inplace=True)
            vix_fut['Cum. Log Return %'] = vix_fut.groupby('Event Datetime')['Log Return %'].cumsum()
           
            sp500_ = get_range(sp500, datetime, range, range, freq)
            sp500_['Price'] = sp500_['Open']
            sp500_['Log Price'] = np.log(sp500_['Price'])
            sp500_['Log Return %'] = sp500_.groupby('Event Datetime')['Log Price'].diff() * 100
            sp500_.fillna(0, inplace=True)
            sp500_['Cum. Log Return %'] = sp500_.groupby('Event Datetime')['Log Return %'].cumsum()
        
            result.append([vix_, vix_fut, sp500_])
        except Exception:
            print(datetime)

    return result


def plot_indiv_fomc(events):
    """
    """
    rows = math.ceil(len(events) / 2)
    fig, axs = plt.subplots(rows, 2, figsize=(15, 4 * rows)) 
    axs = axs.flatten()

    for i, fomc in enumerate(events):
        ax = axs[i] 
        ax.plot(fomc[0]['Datetime Label'], fomc[0]['Cum. % Change'],  label='VIX Cum. Change', color=palette[0])
        ax.plot(fomc[1]['Datetime Label'], fomc[1]['Cum. Log Return %'], label='VIX Future Cum. Log Return', color=palette[1])
        ax.plot(fomc[2]['Datetime Label'], fomc[2]['Cum. Log Return %'], label='S&P 500 Cum. Log Return', color=palette[2])
        ax.set_xlabel('Trading Hours to/from Event', fontsize=12)
        ax.set_ylabel("%", fontsize=12)
        ax.legend(loc='best')
        ax.set_title(f"{fomc[0].index[0][0]}")

    plt.tight_layout()
    plt.show()


def get_fedwatchprob(watch_date, num_upcoming):

    """
    Return the estimated fed watch probabilities given the watch_date and the number of up_coming
    fomc meeting

    Parameters
    ----------
    watch_date: datetime
        The date to calculate the estimated fed watch proability on
    num_upcoming: int 
        The number of foward looking fed watch probaility to calculate
    """
    try: 
        fedwatch = fw.fedwatch.FedWatch(watch_date = watch_date,
                                        fomc_dates = FOMC_DATES,
                                        num_upcoming = num_upcoming,
                                        user_func = read_price_history,
                                        path = BASE_URL_FEDFUT)
        data = fedwatch.generate_hike_info(rate_cols=True)
        data = data.reset_index()
        return data 
    except Exception:
        print(f"Issues with {watch_date}")


def plot_cs_avg(df_list, title):
    """
    """
    fig, ax1 = plt.subplots(figsize=(8,5))
    handles, labels = ax1.get_legend_handles_labels()
    ax2 = ax1.twinx()
    ax1.plot(df_list[0]['Datetime Label'], df_list[0]['Cum. % Change'], color=palette[0], label='VIX Cum. Change')
    ax1.plot(df_list[1]['Datetime Label'], df_list[1]['Cum. Log Return %'], color=palette[1], label='VIX Future Cum. Log Return')
    ax2.plot(df_list[2]['Datetime Label'], df_list[2]['Cum. Log Return %'], color=palette[2], label='S&P 500 Cum. Log Return')
    for x in [-5, -13, -21, 3, 11, 19]:
        ax2.axvline(x=x, linestyle='--', linewidth=0.5, color='k')
        ax2.axvline(x=x-1, linestyle='--', linewidth=0.5, color='k')
    ax2.grid(None)
    ax1.set_xlabel('Trading Hours to/from Event', fontsize=12)
    ax1.set_ylabel("% VIX & VIX Futures", fontsize=12)
    ax2.set_ylabel("% S&P 500")
    ax1.set_title(title)
    ax1.legend(loc='upper left', framealpha=0.5)
    ax2.legend(loc='lower right', framealpha=0.5)
    plt.show()


def get_cs_avg_new(vix_df, sp_df, event_dt, fomc_pc, roll, range, freq):
    """
    """
    vix_fut_filt = get_range_multi(event_dt, roll, range, freq)
    vix_fut_filt['Price'] = (vix_fut_filt['Close Bid Price'] + vix_fut_filt['Close Ask Price']) / 2
    vix_fut_filt = vix_fut_filt[['Symbol', 'Open Time', 'Close Time', 'Price', 'Datetime Label']]
    vix_fut_filt['Log Price'] = np.log(vix_fut_filt['Price'])
    vix_fut_filt['Log Return %'] = vix_fut_filt.groupby('Event Datetime')['Log Price'].diff() * 100
    vix_fut_filt.fillna(0, inplace=True)
    vix_fut_filt['Cum. Log Return %'] = vix_fut_filt.groupby('Event Datetime')['Log Return %'].cumsum()
    vix_fut_filt_ca = vix_fut_filt.groupby('Datetime Label')['Cum. Log Return %'].mean().reset_index()

    vix_filt = get_range(vix_df, event_dt, range, range, freq)
    vix_filt['% Change'] = vix_filt.groupby('Event Datetime')['Price'].pct_change() * 100
    vix_filt.fillna(0, inplace=True)
    vix_filt['Cum. % Change'] = vix_filt.groupby('Event Datetime')['% Change'].cumsum()
    vix_filt = vix_filt.join(fomc_pc, how='left', on='Event Datetime')
    vix_filt_ca = vix_filt.groupby('Datetime Label')['Cum. % Change'].mean().reset_index()
    
    sp500_filt = get_range(sp_df, event_dt, range, range, freq)
    sp500_filt['Price'] = sp500_filt['Open']
    sp500_filt['Log Price'] = np.log(sp500_filt['Price'])
    sp500_filt['Log Return %'] = sp500_filt.groupby('Event Datetime')['Log Price'].diff() * 100
    sp500_filt.fillna(0, inplace=True)
    sp500_filt['Cum. Log Return %'] = sp500_filt.groupby('Event Datetime')['Log Return %'].cumsum()
    sp500_filt_ca = sp500_filt.groupby('Datetime Label')['Cum. Log Return %'].mean().reset_index()

    return [[vix_filt, vix_fut_filt, sp500_filt],
            [vix_filt_ca, vix_fut_filt_ca, sp500_filt_ca]]
    

def get_panel_bucket(df_list, start_label, end_label):
    """
    Return panel data within time label and the corresponding clusters
    """
    filt_list = []
    for df in df_list:
        filt = df[df['Datetime Label'].isin([i for i in range(start_label, end_label + 1 , 1)])]
        filt_list.append(filt)
    panel = pd.concat([filt_list[2]['Cum. Log Return %'], filt_list[0]['Cum. % Change'], filt_list[1]['Cum. Log Return %'], filt_list[0]['PC'], filt_list[0]['Datetime Label']], axis=1)
    panel.columns = ['Cum_Log_Return_SP500', 'Cum_Change_VIX', 'Cum_Log_Return_VIX_Fut', 'PC', 'Datetime Label']
    panel.reset_index(inplace=True)
    panel.set_index(['Event Datetime', 'Datetime Label'], inplace=True)
    panel['c1'] = panel.index.get_level_values('Event Datetime')
    panel['c2'] = panel.index.get_level_values('Datetime Label')
    clusters = pd.concat([panel['c1'], panel['c2']], axis=1)
    return panel, clusters
 