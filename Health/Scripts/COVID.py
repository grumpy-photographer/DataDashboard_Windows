import pandas as pd

df = pd.read_csv('C:/Users/natha/OneDrive/Desktop/GitHub/covid-19-data/us-counties.csv')

filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

df['fips'] = df['fips'].astype(int)

df_fips = pd.read_csv('./FIPS_Codes.csv')

df = df.set_index("fips").join(df_fips.set_index("GeoFIPS"))

df.to_csv('./Updates/covid_data.csv', sep='\t')
df.to_csv('./Updates/covid_data.txt', sep='\t')