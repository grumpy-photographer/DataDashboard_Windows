#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests

# Create backups
df_backup = pd.read_csv('./Updates/STG_BEA_MSALESUSETAX_0002.txt')
df_backup.to_csv('./Backups/STG_BEA_MSALESUSETAX_0002_BACKUP.txt')

print('Make sure to update the file list!!')

files = {
    '2018M10': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-18.xls',
    '2018M11': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_11-18.xls',
    '2018M12': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_12-18.xls'
    }

for key, value in files.items():
    df = pd.read_excel(value, skiprows = 9)

    # Drop NaN row
    df = df.drop(df.index[0])
    df = df[:-8]

    # Remove columns with amiguous names
    df = df.loc[:,~df.columns.str.contains('Unnamed')]

    # Create new dataframe with second set of counties
    df1 = pd.DataFrame(df, columns = ['County.1', 'Sales*.1'])

    # Drop second set of counties from original dataframe
    df = df.drop(columns=['County.1', 'Sales*.1', 'Collections*', 'Collections*.1'])

    # Rename columns
    df = df.rename(columns = {'County.1':'County', 'Sales*':'Sales'})
    df1 = df1.rename(columns = {'County.1':'County', 'Sales*.1':'Sales'})

    # append dataframes
    df_list = [df, df1]
    df_master1 = df.append(df_list)
    df_master1['Data_Period_Business_Key'] = key

    # Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
    df_master1 = df_master1.dropna(how='all')
    df_master1 = df_master1.fillna('0')

    # Change dtypes to Int
    df_master1['Sales'] = df_master1['Sales'].astype(float)

    # Drop junk rows
    df_master1 = df_master1[:-5]


files2 = {
    '2019M01': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_1-19.xls',
    '2019M02': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_2-19.xls',
    '2019M03': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_3-19.xls',
    '2019M04': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_4-19.xls',
    '2019M05': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_5-19.xls',
    '2019M06': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_6-19.xls',
    '2019M07': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_7-19.xls',
    '2019M08': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_8-19.xls',
    '2019M09': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_9-19.xls',
    '2019M10': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-19.xls',
    '2019M11': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_11-19.xls',
    '2019M12': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_12-19.xls',
    '2020M01': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_1-20.xls',
    '2020M02': 'https://files.nc.gov/ncdor/documents/reports/monthly_sales_2-20.xls'
    }

for key, value in files2.items():
    df = pd.read_excel(value, skiprows = 9)

    # Drop NaN row
    df = df.drop(df.index[0])
    df = df[:-8]

    # Remove columns with amiguous names
    df = df.loc[:,~df.columns.str.contains('Unnamed')]

    # Create new dataframe with second set of counties
    df1 = pd.DataFrame(df, columns = ['County.1', 'and Purchases*.1'])

    # Drop second set of counties from original dataframe
    df = df.drop(columns=['County.1', 'and Purchases*.1', 'Collections*', 'Collections*.1'])

    # Rename columns
    df = df.rename(columns = {'County.1':'County', 'and Purchases*':'Sales'})
    df1 = df1.rename(columns = {'County.1':'County', 'and Purchases*.1':'Sales'})

    # append dataframes
    df_list = [df, df1]
    df_master2 = df.append(df_list)
    df_master2['Data_Period_Business_Key'] = key

    # Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
    df_master2 = df_master2.dropna(how='all')
    df_master2 = df_master2.fillna('0')

    # Change dtypes to Int
    df_master2['Sales'] = df_master2['Sales'].astype(float)

    # Drop junk rows
    df_master2 = df_master2[:-5]

df_master_list = [df_master1, df_master2]
df_master = df_master1.append(df_master_list)

df_master = df_master.sort_values(by = 'County', ascending=True)
df_master['Sales'] = df_master['Sales'].round(0)
df_master = df_master.set_index('County')


df_master.to_csv('./Updates/STG_BEA_MSALESUSETAX_0002.txt', sep='\t')