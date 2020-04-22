import pandas as pd
import subprocess

repo = './Data/covid-19-data'

subprocess.call(['git','fetch', repo])
subprocess.call(['git','pull', repo])

df = pd.read_csv('./Data/covid-19-data/us-counties.csv')

filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

df['fips'] = df['fips'].astype(int)

df = df.rename(columns = {"fips":'GeoArea_FIPS', 'county':'GeoArea_Name', 'cases':'Cases', 'date':'Data_Period_Business_Key'})
df = df.drop(['deaths', 'state'], axis=1)

columns = ['GeoArea_FIPS', 'GeoArea_Name', 'Data_Period_Business_Key', 'Cases']
df = df[columns]

df.set_index('GeoArea_FIPS', inplace =True)

df.to_csv('./Updates/covid_data.txt', sep='\t')