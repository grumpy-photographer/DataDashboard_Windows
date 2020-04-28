import urllib
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc
import datetime as dt

#### Backups ####
df_backup = pd.read_csv('./Updates/STG_WNCD_Health_Data_Series.txt', sep='\t')
df_backup.to_csv('./Updates/STG_WNCD_Health_Data_Series_BACKUP.txt', sep='\t')

#### COVID ####
files = {
    'NYTI_CNTY_COV02':'./Updates/STG_NYTI_CNTY_COVID_19_Deaths.txt',
    'NYTI_NAT_COV03':'./Updates/STG_NYTI_NAT_COVID_19_Cases.txt',
    'NYTI_NAT_COV04':'./Updates/STG_NYTI_NAT_COVID_19_Deaths.txt'
    }

df = pd.read_csv('./Updates/STG_NYTI_CNTY_COVID_19_Cases.txt', sep='\t')
df['Measure_Name'] = 'NYTI_CNTY_COV01'

for key, value in files.items():
    df1 = pd.read_csv(value, sep='\t')
    df1['Measure_Name'] = key
    df_append = df.append(df1)
    pass


#### Save Data Series ####
df_append.to_csv('./Updates/STG_WNCD_Health_Data_Series.txt', sep='\t')

#### Upload to Data Base ####
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=GREENSPAN\ECONPROD;"
    "Database=Final_Copy;"
    "Trusted_Connection=yes;",
    autocommit=True,
    )

c = con.cursor()

c.execute("drop table STG_WNCD_Health_Data_Series_BACKUP")

c.execute("""sp_rename 'STG_WNCD_Health_Data_Series', 'STG_WNCD_Health_Data_Series_BACKUP';""")

params = urllib.parse.quote_plus(
    r"Driver={SQL Server};"
    r"Server=GREENSPAN\ECONPROD;"
    r"Database=Final_Copy;"
    r"Trusted_Connection=yes;"
    )

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df_append.to_sql(
    "STG_WNCD_Health_Data_Series", con=engine, if_exists="replace", index=False
    )
