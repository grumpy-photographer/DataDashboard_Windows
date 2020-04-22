import pandas as pd

df = pd.read_csv('C:/Users/natha/OneDrive/Desktop/GitHub/covid-19-data/us-counties.csv')

filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

df['fips'] = df['fips'].astype(int)

df = pd.rename(df['fips':'GeoArea_FIPS'])

df_new = pd.read_excel('../TableauData_NC_Blank_Section.xlsx')

print(df_new)

df.to_csv('./Updates/covid_data.txt', sep='\t')