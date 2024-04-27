# How to Can we compute a VRP at the daily frequency? 

# Cannot find VRP online?
# Bloomberg's historical volatility is different from what the paper proposes

# In the orginal paper, sample ranges from Jan 1990 to December 2007

# The implied variance part of the VRP is measured with monthly VIX squared 
# as CBOE VIX is the industry standard. Though there are changed to how CBOE
# calculates the VIX throughout the years
# The daily proportion can be taken from the closing tick with Prof Cheng's VIX tick data

# The realized variance part of the VRP is measured with equation 21), 
# using S&P 500 index data on a 5-minute sampling frequency. 
# From there, a monthly measure of realized variance is calculated
# using 22 days of 78 within-day 5 minute squared returns. Thus n=22x78
# For daily, we could use the same method but 
# then a 5 minute sampling frequency might be too low