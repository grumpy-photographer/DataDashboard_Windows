import pandas as pd

df = pd.read_csv('C:/Users/natha/OneDrive/Desktop/GitHub/covid-19-data/us-counties.csv')

filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

df['fips'] = df['fips'].astype(int)

df = df.rename(columns = {"fips":'GeoArea_FIPS', 'county':'GeoArea_Name', 'cases':'Cases', 'date':'Data_Period_Business_Key'})
df = df.drop(['deaths', 'state'], axis=1)

df.set_index('GeoArea_FIPS', inplace =True)

df = df.columns('GeoArea_FIPS', 'GeoArea_Name', 'Data_Period_Business_Key', 'Cases')

df.to_csv('./Updates/covid_data.txt', sep='\t')