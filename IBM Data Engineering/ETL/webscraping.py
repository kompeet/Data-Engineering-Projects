from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd

# Extract Data Using Web Scraping
# The wikipedia webpage https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks provides information about largest banks in the world by various parameters.
# Scrape the data from the table 'By market capitalization' and store it in a JSON file.

url = 'https://web.archive.org/web/20200318083015/https://en.wikipedia.org/wiki/List_of_largest_banks'
response = requests.get(url)
html_data = response.text

# print(html_data)
# print(html_data[760:783])

soup = BeautifulSoup(html_data, 'html.parser')
# soup.prettify()
# print(soup)


# Load the data from the By market capitalization table into a pandas dataframe. The dataframe should have the bank Name and Market Cap (US$ Billion) as column names. 
# Using the empty dataframe data and the given loop extract the necessary data from each row and append it to the empty dataframe.

data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])

for row in soup.find_all('tbody')[2].find_all('tr'):
    col = row.find_all('td')
    if len(col) > 0:
       name = col[1].text.strip()
       market_cap = float(col[2].string.strip())
       data = data._append({"Name": name, "Market Cap (US$ Billion)": market_cap}, ignore_index=True)
       
       
# Load the pandas dataframe into a JSON.
       
print(data.head())
data.to_json("bank_market_cap.json")





