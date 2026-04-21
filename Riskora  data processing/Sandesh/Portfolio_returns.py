import numpy as np 
import pandas as pd

#Load combined data 
data = pd.read_csv("clean_data.csv", index_col="Date", parse_dates=True)

#Calculate Daily Returns
returns=data.pct_change().dropna()

#Equal weights
num_stocks=len(returns.columns)
weights=np.ones(num_stocks)/num_stocks

#Annual Expected Return 
expected_return = np.dot(returns.mean(), weights)*252
print("Annual Expected Return:", round(expected_return,4))
returns.to_csv("portfolio_returns.csv")