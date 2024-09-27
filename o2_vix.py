import pandas as pd 
import pyfedwatch as fw
import scipy
import matplotlib.pyplot as plt
from pyfedwatch.datareader import read_price_history
from pyfedwatch.datareader import get_fomc_data


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
    Return a dataframe containing the cross sectional average for each of the intervals,
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
        # result = pd.concat([result, data]).fillna(0)
    except Exception:
        print(f"Issues with {watch_date}")