#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests

# Create backups
df_backup = pd.read_csv('./Updates/STG_BEA_MSALESUSETAX_0002.txt')
df_backup.to_csv('./Backups/STG_BEA_MSALESUSETAX_0002_BACKUP.txt')


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-18.xls', skiprows = 9)
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
df_master0 = df.append(df_list)
df_master0['Data_Period_Business_Key'] = '2018M10'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master0 = df_master0.dropna(how='all')
df_master0 = df_master0.fillna('0')
# Change dtypes to Int
df_master0['Sales'] = df_master0['Sales'].astype(float)
# Drop junk rows
df_master0 = df_master0[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_11-18.xls', skiprows = 9)
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
df_master1['Data_Period_Business_Key'] = '2018M11'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master1 = df_master1.dropna(how='all')
df_master1 = df_master1.fillna('0')
# Change dtypes to Int
df_master1['Sales'] = df_master1['Sales'].astype(float)
# Drop junk rows
df_master1 = df_master1[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_12-18.xls', skiprows = 9)
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
df_master2 = df.append(df_list)
df_master2['Data_Period_Business_Key'] = '2018M12'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master2 = df_master2.dropna(how='all')
df_master2 = df_master2.fillna('0')
# Change dtypes to Int
df_master2['Sales'] = df_master2['Sales'].astype(float)
# Drop junk rows
df_master2 = df_master2[:-10]




df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_1-19.xls', skiprows = 9)
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
df_master3 = df.append(df_list)
df_master3['Data_Period_Business_Key'] = '2019M01'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master3 = df_master3.dropna(how='all')
df_master3 = df_master3.fillna('0')
# Change dtypes to Int
df_master3['Sales'] = df_master3['Sales'].astype(float)
# Drop junk rows
df_master3 = df_master3[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_2-19.xls', skiprows = 9)
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
df_master4 = df.append(df_list)
df_master4['Data_Period_Business_Key'] = '2019M02'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master4 = df_master4.dropna(how='all')
df_master4 = df_master4.fillna('0')
# Change dtypes to Int
df_master4['Sales'] = df_master4['Sales'].astype(float)
# Drop junk rows
df_master4 = df_master4[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_3-19.xls', skiprows = 9)
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
df_master5 = df.append(df_list)
df_master5['Data_Period_Business_Key'] = '2019M03'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master5 = df_master5.dropna(how='all')
df_master5 = df_master5.fillna('0')
# Change dtypes to Int
df_master5['Sales'] = df_master5['Sales'].astype(float)
# Drop junk rows
df_master5 = df_master5[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_4-19.xls', skiprows = 9)
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
df_master6 = df.append(df_list)
df_master6['Data_Period_Business_Key'] = '2019M04'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master6 = df_master6.dropna(how='all')
df_master6 = df_master6.fillna('0')
# Change dtypes to Int
df_master6['Sales'] = df_master6['Sales'].astype(float)
# Drop junk rows
df_master6 = df_master6[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_5-19.xls', skiprows = 9)
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
df_master7 = df.append(df_list)
df_master7['Data_Period_Business_Key'] = '2019M05'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master7 = df_master7.dropna(how='all')
df_master7 = df_master7.fillna('0')
# Change dtypes to Int
df_master7['Sales'] = df_master7['Sales'].astype(float)
# Drop junk rows
df_master7 = df_master7[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_6-19.xls', skiprows = 9)
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
df_master8 = df.append(df_list)
df_master8['Data_Period_Business_Key'] = '2019M06'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master8 = df_master8.dropna(how='all')
df_master8 = df_master8.fillna('0')
# Change dtypes to Int
df_master8['Sales'] = df_master8['Sales'].astype(float)
# Drop junk rows
df_master8 = df_master8[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_7-19.xls', skiprows = 9)
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
df_master9 = df.append(df_list)
df_master9['Data_Period_Business_Key'] = '2019M07'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master9 = df_master9.dropna(how='all')
df_master9 = df_master9.fillna('0')
# Change dtypes to Int
df_master9['Sales'] = df_master9['Sales'].astype(float)
# Drop junk rows
df_master9 = df_master9[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_8-19.xls', skiprows = 9)
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
df_master10 = df.append(df_list)
df_master10['Data_Period_Business_Key'] = '2019M08'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master10 = df_master10.dropna(how='all')
df_master10 = df_master10.fillna('0')
# Change dtypes to Int
df_master10['Sales'] = df_master10['Sales'].astype(float)
# Drop junk rows
df_master10 = df_master10[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_9-19.xls', skiprows = 9)
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
df_master11 = df.append(df_list)
df_master11['Data_Period_Business_Key'] = '2019M09'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master11 = df_master11.dropna(how='all')
df_master11 = df_master11.fillna('0')
# Change dtypes to Int
df_master11['Sales'] = df_master11['Sales'].astype(float)
# Drop junk rows
df_master11 = df_master11[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_10-19.xls', skiprows = 9)
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
df_master12 = df.append(df_list)
df_master12['Data_Period_Business_Key'] = '2019M10'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master12 = df_master12.dropna(how='all')
df_master12 = df_master12.fillna('0')
# Change dtypes to Int
df_master12['Sales'] = df_master12['Sales'].astype(float)
# Drop junk rows
df_master12 = df_master12[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_11-19.xls', skiprows = 9)
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
df_master13 = df.append(df_list)
df_master13['Data_Period_Business_Key'] = '2019M11'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master13 = df_master13.dropna(how='all')
df_master13 = df_master13.fillna('0')
# Change dtypes to Int
df_master13['Sales'] = df_master13['Sales'].astype(float)
# Drop junk rows
df_master13 = df_master13[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_12-19.xls', skiprows = 9)
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
df_master14 = df.append(df_list)
df_master14['Data_Period_Business_Key'] = '2019M12'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master14 = df_master14.dropna(how='all')
df_master14 = df_master14.fillna('0')
# Change dtypes to Int
df_master14['Sales'] = df_master14['Sales'].astype(float)
# Drop junk rows
df_master14 = df_master14[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_1-20.xls', skiprows = 9)
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
df_master15 = df.append(df_list)
df_master15['Data_Period_Business_Key'] = '2020M01'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master15 = df_master15.dropna(how='all')
df_master15 = df_master15.fillna('0')
# Change dtypes to Int
df_master15['Sales'] = df_master15['Sales'].astype(float)
# Drop junk rows
df_master15 = df_master15[:-10]


df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_2-20.xls', skiprows = 9)
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
df_master16 = df.append(df_list)
df_master16['Data_Period_Business_Key'] = '2020M02'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master16 = df_master16.dropna(how='all')
df_master16 = df_master16.fillna('0')
# Change dtypes to Int
df_master16['Sales'] = df_master16['Sales'].astype(float)
# Drop junk rows
df_master16 = df_master16[:-10]


'''df = pd.read_excel('https://files.nc.gov/ncdor/documents/reports/monthly_sales_3-20.xls', skiprows = 9)
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
df = df.rename(columns = {'County.1':'County', 'and Purcahses*':'Sales'})
df1 = df1.rename(columns = {'County.1':'County', 'and Purchases*.1':'Sales'})
# append dataframes
df_list = [df, df1]
df_master17 = df.append(df_list)
df_master17['Data_Period_Business_Key'] = '2020M03'
# Drop rows that are all NaN and replace NaN with 0 if the entire row is not NaN
df_master17 = df_master17.dropna(how='all')
df_master17 = df_master17.fillna('0')
# Change dtypes to Int
df_master17['Sales'] = df_master17['Sales'].astype(float)
# Drop junk rows
df_master17 = df_master17[:-10]'''

df_master_list = [df_master1, df_master2, df_master3, df_master4, df_master5, df_master6, df_master7, df_master8, df_master9, df_master10, df_master11, df_master12, df_master13, df_master14, df_master15, df_master16]
df_master = df_master0.append(df_master_list)

df_master = df_master.sort_values(by = ['County','Data_Period_Business_Key'], ascending=True)
df_master['Sales'] = df_master['Sales'].round(0)
df_master = df_master.set_index('County')
df_master = df_master.drop(df_master.index[[0]])
df_master = df_master.drop_duplicates()

df_master.to_csv('./Updates/STG_BEA_MSALESUSETAX_0002.txt', sep='\t')