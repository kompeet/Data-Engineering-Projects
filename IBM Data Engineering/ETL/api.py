import requests
import pandas as pd

# Exchange Rates Data API
url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=rTNadIvh1nus3O41kUuG8sn94Sw2Gh4V"
response = requests.get(url)
api_data = response.text
data = response.json()
# print(api_data)
# print(data)

# The dataframe should have the Currency as the index and Rate as their columns. 

df = pd.DataFrame.from_dict(data)
df.index.name = 'Currency'
df = df.drop(['success', 'timestamp', 'base', 'date'], axis=1)
print(df)


df.to_csv('exchange_rates_1.csv')






