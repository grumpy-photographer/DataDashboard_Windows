import pandas as pd
import subprocess

#read data
df = pd.read_csv('./Data/covid-19-data/us-counties.csv')

#fitler to nc
filter1 = df['state'].str.contains('North Carolina')
df = df[filter1]

#clean
df['fips'] = df['fips'].astype(int)
df = df.rename(columns = {"fips":'GeoArea_FIPS', 'county':'GeoArea_Name', 'cases':'Estimated_Value', 'date':'Data_Period_Business_Key'})
df = df.drop(['deaths', 'state'], axis=1)

#add missing columns to match database
df['Economic_Measure_Code'] = 'NYTI_CNTY_COV01'
df['Economic_Measure_Name'] = 'COVID-19 Confirmed Cases'
df['Measure_Name'] = ''
df['Unit_of_Measure_Code'] = 'Count'

#reset columns
columns = ['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Measure_Name', 'Data_Period_Business_Key', 'Estimated_Value']
df = df[columns]
df.set_index('GeoArea_FIPS', inplace =True)



#save as txt
df.to_csv('./Updates/STG_NYTI_CNTY_COVID_19_Cases.txt', sep='\t')