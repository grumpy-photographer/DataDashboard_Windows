import pandas as pd

df = pd.read_csv('C:/Users/natha/OneDrive/Desktop/GitHub/covid-19-data/us-counties.csv')

filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

df['fips'] = df['fips'].astype(int)

df.to_csv('covid_data.csv', sep='\t')
df.to_csv('covid_data.txt', sep='\t')