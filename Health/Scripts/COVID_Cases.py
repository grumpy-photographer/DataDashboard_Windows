import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

#create backups
df_backup = pd.read_csv('./Updates/STG_NYTI_CNTY_COVID_19_Cases.txt', sep='\t')
df_backup.to_csv('./Backups/STG_NYTI_CNTY_COVID_19_Cases_BACKUP.txt', sep='\t')

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

#upload to database 
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=TITANIUM-BOOK;"
    "Database=DataDashboard;"
    "Trusted_Connection=yes;",
    autocommit=True,
)

c = con.cursor()

#create new backup
c.execute("drop table STG_NYTI_CNTY_COVID_19_Cases_BACKUP")
c.execute(    
    """sp_rename 'dbo.STG_NYTI_CNTY_COVID_19_Cases',
    'STG_NYTI_CNTY_COVID_19_Cases_BACKUP';"""
    )

c.execute("""USE [Final_Copy]

SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

CREATE TABLE [dbo].[STG_NYTI_CNTY_COVID_19_Cases](
	[GeoArea_FIPS] [varchar](50) NULL,
	[GeoArea_Name] [varchar](50) NULL,
	[Economic_Measure_Code] [varchar](50) NULL,
	[Economic_Measure_Name] [varchar](50) NULL,
	[Measure_Name] [varchar](50) NULL,
	[Data_Period_Business_Key] [varchar](50) NULL,
	[Estimated_Value] [varchar](50) NULL
) ON [PRIMARY]
""")

params = urllib.parse.quote_plus(r"Driver={SQL Server};"r"Server=TITANIUM-BOOK;"r"Database=DataDashboard;"r"Trusted_Connection=yes;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True)
df.to_sql("STG_NYTI_CNTY_COVID_19_Cases", con=engine, if_exists="replace", index=False)