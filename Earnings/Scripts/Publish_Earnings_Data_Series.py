import urllib
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc
import datetime as dt


backup_df = pd.read_csv('../Updates/STG_WNCD_Earnings_Data_Series.txt', sep='\t')
backup_df.to_csv('../Backups/STG_WNCD_Earnings_Data_Series_BACKUP.txt', sep='\t')


#### BEA ####

df1 = pd.read_csv('../Updates/STG_BEA_CA5N_Population.txt', sep='\t')
df1['Economic_Measure_Code'] = 'BEA_CA5N_0020'
df1['Published_UOM'] = 'UNIT'
df1['Estimation_Qualifier'] = ''
df1['Unit_of_Measure_Code'] = ''
df1['Default_Scale'] = '0'
df1['Calculation_Type'] = 'Level'
df1['County_FIPS'] = df1['GeoFIPS']
df1['County_Name'] = df1['GeoName']
df1['Measure_Hierarchy_Level'] = '1'


df2 = pd.read_csv('../Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt', sep='\t')
df2['Economic_Measure_Code'] = 'BEA_CA5N_0030'
df2['Published_UOM'] = 'N$'
df2['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df2['Unit_of_Measure_Code'] = 'R$'
df2['Default_Scale'] = '0'
df2['Calculation_Type'] = 'Ratio'
df2['County_FIPS'] = df2['GeoFIPS']
df2['County_Name'] = df2['GeoName']
df2['Measure_Hierarchy_Level'] = '1'


df3 = pd.read_csv('../Updates/STG_BEA_CA5N_Personal_Income.txt', sep='\t')
df3['Economic_Measure_Code'] = 'BEA_CA5N_0010'
df3['Published_UOM'] = 'N$'
df3['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df3['Unit_of_Measure_Code'] = 'R$'
df3['Default_Scale'] = '-3'
df3['Calculation_Type'] = 'Level'
df3['County_FIPS'] = df3['GeoFIPS']
df3['County_Name'] = df3['GeoName']
df3['Measure_Hierarchy_Level'] = '1'


df4 = pd.read_csv('../Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt', sep='\t')
df4['Economic_Measure_Code'] = 'BEA_CA5N_0035'
df4['Published_UOM'] = 'N$'
df4['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df4['Unit_of_Measure_Code'] = 'R$'
df4['Default_Scale'] = '-3'
df4['Calculation_Type'] = 'Level'
df4['County_FIPS'] = df4['GeoFIPS']
df4['County_Name'] = df4['GeoName']
df4['Measure_Hierarchy_Level'] = '1'


df5 = pd.read_csv("../Updates/STG_BEA_CA5N_Wages_and_Salaries.txt", sep='\t')
df5['Economic_Measure_Code'] = 'BEA_CA5N_0050'
df5['Published_UOM'] = 'N$'
df5['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df5['Unit_of_Measure_Code'] = 'R$'
df5['Default_Scale'] = '-3'
df5['Calculation_Type'] = 'Level'
df5['County_FIPS'] = df5['GeoFIPS']
df5['County_Name'] = df5['GeoName']
df5['Measure_Hierarchy_Level'] = '2'


df6 = pd.read_csv('../Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt', sep='\t')
df6['Economic_Measure_Code'] = 'BEA_CA5N_0060'
df6['Published_UOM'] = 'N$'
df6['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df6['Unit_of_Measure_Code'] = 'R$'
df6['Default_Scale'] = '-3'
df6['Calculation_Type'] = 'Level'
df6['County_FIPS'] = df6['GeoFIPS']
df6['County_Name'] = df6['GeoName']
df6['Measure_Hierarchy_Level'] = '2'


df7 = pd.read_csv('../Updates/STG_BEA_CA5N_Proprietors_Income.txt', sep='\t')
df7['Economic_Measure_Code'] = 'BEA_CA5N_0070'
df7['Published_UOM'] = 'N$'
df7['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df7['Unit_of_Measure_Code'] = 'R$'
df7['Default_Scale'] = '-3'
df7['Calculation_Type'] = 'Level'
df7['County_FIPS'] = df7['GeoFIPS']
df7['County_Name'] = df7['GeoName']
df7['Measure_Hierarchy_Level'] = '2'


df8 = pd.read_csv('../Updates/STG_BEA_CA5N_Farm_Earnings.txt', sep='\t')
df8['Economic_Measure_Code'] = 'BEA_CA5N_0081'
df8['Published_UOM'] = 'N$'
df8['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df8['Unit_of_Measure_Code'] = 'R$'
df8['Default_Scale'] = '-3'
df8['Calculation_Type'] = 'Level'
df8['County_FIPS'] = df8['GeoFIPS']
df8['County_Name'] = df8['GeoName']
df8['Measure_Hierarchy_Level'] = '2'


df9 = pd.read_csv('../Updates/STG_BEA_CA5N_Private_Nonfarm_Earnings.txt', sep='\t')
df9['Economic_Measure_Code'] = 'BEA_CA5N_0082'
df9['Published_UOM'] = 'N$'
df9['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df9['Unit_of_Measure_Code'] = 'R$'
df9['Default_Scale'] = '-3'
df9['Calculation_Type'] = 'Level'
df9['County_FIPS'] = df9['GeoFIPS']
df9['County_Name'] = df9['GeoName']
df9['Measure_Hierarchy_Level'] = '2'


df10 = pd.read_csv('../Updates/STG_BEA_CA5N_Private_Nonfarm_Earnings.txt', sep='\t')
df10['Economic_Measure_Code'] = 'BEA_CA5N_0090'
df10['Published_UOM'] = 'N$'
df10['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df10['Unit_of_Measure_Code'] = 'R$'
df10['Default_Scale'] = '-3'
df10['Calculation_Type'] = 'Level'
df10['County_FIPS'] = df10['GeoFIPS']
df10['County_Name'] = df10['GeoName']
df10['Measure_Hierarchy_Level'] = '3'


df11 = pd.read_csv('../Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt', sep='\t')
df11['Economic_Measure_Code'] = 'BEA_CA5N_0100'
df11['Published_UOM'] = 'N$'
df11['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df11['Unit_of_Measure_Code'] = 'R$'
df11['Default_Scale'] = '-3'
df11['Calculation_Type'] = 'Level'
df11['County_FIPS'] = df11['GeoFIPS']
df11['County_Name'] = df11['GeoName']
df11['Measure_Hierarchy_Level'] = '4'


df12 = pd.read_csv('../Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', sep='\t')
df12['Economic_Measure_Code'] = 'BEA_CA5N_0200  '
df12['Published_UOM'] = 'N$'
df12['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df12['Unit_of_Measure_Code'] = 'R$'
df12['Default_Scale'] = '-3'
df12['Calculation_Type'] = 'Level'
df12['County_FIPS'] = df12['GeoFIPS']
df12['County_Name'] = df12['GeoName']
df12['Measure_Hierarchy_Level'] = '4'


df13 = pd.read_csv('../Updates/STG_BEA_CA5N_Utilities.txt', sep='\t')
df13['Economic_Measure_Code'] = 'BEA_CA5N_0300'
df13['Published_UOM'] = 'N$'
df13['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df13['Unit_of_Measure_Code'] = 'R$'
df13['Default_Scale'] = '-3'
df13['Calculation_Type'] = 'Level'
df13['County_FIPS'] = df13['GeoFIPS']
df13['County_Name'] = df13['GeoName']
df13['Measure_Hierarchy_Level'] = '4'


df14 = pd.read_csv('../Updates/STG_BEA_CA5N_Construction.txt', sep='\t')
df14['Economic_Measure_Code'] = 'BEA_CA5N_0400'
df14['Published_UOM'] = 'N$'
df14['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df14['Unit_of_Measure_Code'] = 'R$'
df14['Default_Scale'] = '-3'
df14['Calculation_Type'] = 'Level'
df14['County_FIPS'] = df14['GeoFIPS']
df14['County_Name'] = df14['GeoName']
df14['Measure_Hierarchy_Level'] = '4'
   

df15 = pd.read_csv('../Updates/STG_BEA_CA5N_Manufacturing.txt', sep='\t')
df15['Economic_Measure_Code'] = 'BEA_CA5N_0500'
df15['Published_UOM'] = 'N$'
df15['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df15['Unit_of_Measure_Code'] = 'R$'
df15['Default_Scale'] = '-3'
df15['Calculation_Type'] = 'Level'
df15['County_FIPS'] = df15['GeoFIPS']
df15['County_Name'] = df15['GeoName']
df15['Measure_Hierarchy_Level'] = '4'


df16 = pd.read_csv('../Updates/STG_BEA_CA5N_Wholesale_Trade.txt', sep='\t')
df16['Economic_Measure_Code'] = 'BEA_CA5N_0600'
df16['Published_UOM'] = 'N$'
df16['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df16['Unit_of_Measure_Code'] = 'R$'
df16['Default_Scale'] = '-3'
df16['Calculation_Type'] = 'Level'
df16['County_FIPS'] = df16['GeoFIPS']
df16['County_Name'] = df16['GeoName']
df16['Measure_Hierarchy_Level'] = '4'


df17 = pd.read_csv('../Updates/STG_BEA_CA5N_Retail_Trade.txt', sep='\t')
df17['Economic_Measure_Code'] = 'BEA_CA5N_0700'
df17['Published_UOM'] = 'N$'
df17['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df17['Unit_of_Measure_Code'] = 'R$'
df17['Default_Scale'] = '-3'
df17['Calculation_Type'] = 'Level'
df17['County_FIPS'] = df17['GeoFIPS']
df17['County_Name'] = df17['GeoName']
df17['Measure_Hierarchy_Level'] = '4'


df18 = pd.read_csv('../Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt', sep='\t')
df18['Economic_Measure_Code'] = 'BEA_CA5N_0800'
df18['Published_UOM'] = 'N$'
df18['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df18['Unit_of_Measure_Code'] = 'R$'
df18['Default_Scale'] = '-3'
df18['Calculation_Type'] = 'Level'
df18['County_FIPS'] = df18['GeoFIPS']
df18['County_Name'] = df18['GeoName']
df18['Measure_Hierarchy_Level'] = '4'


df19 = pd.read_csv('../Updates/STG_BEA_CA5N_Information.txt', sep='\t')
df19['Economic_Measure_Code'] = 'BEA_CA5N_0900'
df19['Published_UOM'] = 'N$'
df19['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df19['Unit_of_Measure_Code'] = 'R$'
df19['Default_Scale'] = '-3'
df19['Calculation_Type'] = 'Level'
df19['County_FIPS'] = df19['GeoFIPS']
df19['County_Name'] = df19['GeoName']
df19['Measure_Hierarchy_Level'] = '4'


df20 = pd.read_csv('../Updates/STG_BEA_CA5N_Finance_and_Insurance.txt', sep='\t')
df20['Economic_Measure_Code'] = 'BEA_CA5N_1000'
df20['Published_UOM'] = 'N$'
df20['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df20['Unit_of_Measure_Code'] = 'R$'
df20['Default_Scale'] = '-3'
df20['Calculation_Type'] = 'Level'
df20['County_FIPS'] = df20['GeoFIPS']
df20['County_Name'] = df20['GeoName']
df20['Measure_Hierarchy_Level'] = '4'


df21 = pd.read_csv('../Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt', sep='\t')
df21['Economic_Measure_Code'] = 'BEA_CA5N_1100'
df21['Published_UOM'] = 'N$'
df21['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df21['Unit_of_Measure_Code'] = 'R$'
df21['Default_Scale'] = '-3'
df21['Calculation_Type'] = 'Level'
df21['County_FIPS'] = df21['GeoFIPS']
df21['County_Name'] = df21['GeoName']
df21['Measure_Hierarchy_Level'] = '4'


df22 = pd.read_csv('../Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt', sep='\t')
df22['Economic_Measure_Code'] = 'BEA_CA5N_1200'
df22['Published_UOM'] = 'N$'
df22['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df22['Unit_of_Measure_Code'] = 'R$'
df22['Default_Scale'] = '-3'
df22['Calculation_Type'] = 'Level'
df22['County_FIPS'] = df22['GeoFIPS']
df22['County_Name'] = df22['GeoName']
df22['Measure_Hierarchy_Level'] = '4'


df23 = pd.read_csv('../Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt', sep='\t')
df23['Economic_Measure_Code'] = 'BEA_CA5N_1300'
df23['Published_UOM'] = 'N$'
df23['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df23['Unit_of_Measure_Code'] = 'R$'
df23['Default_Scale'] = '-3'
df23['Calculation_Type'] = 'Level'
df23['County_FIPS'] = df23['GeoFIPS']
df23['County_Name'] = df23['GeoName']
df23['Measure_Hierarchy_Level'] = '4'


df24 = pd.read_csv('../Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', sep='\t')
df24['Economic_Measure_Code'] = 'BEA_CA5N_1400'
df24['Published_UOM'] = 'N$'
df24['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df24['Unit_of_Measure_Code'] = 'R$'
df24['Default_Scale'] = '-3'
df24['Calculation_Type'] = 'Level'
df24['County_FIPS'] = df24['GeoFIPS']
df24['County_Name'] = df24['GeoName']
df24['Measure_Hierarchy_Level'] = '4'


df25 = pd.read_csv('../Updates/STG_BEA_CA5N_Educational_Services.txt', sep='\t')
df25['Economic_Measure_Code'] = 'BEA_CA5N_1500'
df25['Published_UOM'] = 'N$'
df25['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df25['Unit_of_Measure_Code'] = 'R$'
df25['Default_Scale'] = '-3'
df25['Calculation_Type'] = 'Level'
df25['County_FIPS'] = df25['GeoFIPS']
df25['County_Name'] = df25['GeoName']
df25['Measure_Hierarchy_Level'] = '4'


df26 = pd.read_csv('../Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt', sep='\t')
df26['Economic_Measure_Code'] = 'BEA_CA5N_1600'
df26['Published_UOM'] = 'N$'
df26['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df26['Unit_of_Measure_Code'] = 'R$'
df26['Default_Scale'] = '-3'
df26['Calculation_Type'] = 'Level'
df26['County_FIPS'] = df26['GeoFIPS']
df26['County_Name'] = df26['GeoName']
df26['Measure_Hierarchy_Level'] = '4'


df27 = pd.read_csv('../Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt', sep='\t')
df27['Economic_Measure_Code'] = 'BEA_CA5N_1700'
df27['Published_UOM'] = 'N$'
df27['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df27['Unit_of_Measure_Code'] = 'R$'
df27['Default_Scale'] = '-3'
df27['Calculation_Type'] = 'Level'
df27['County_FIPS'] = df27['GeoFIPS']
df27['County_Name'] = df27['GeoName']
df27['Measure_Hierarchy_Level'] = '4'


df28 = pd.read_csv('../Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt', sep='\t')
df28['Economic_Measure_Code'] = 'BEA_CA5N_1800'
df28['Published_UOM'] = 'N$'
df28['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df28['Unit_of_Measure_Code'] = 'R$'
df28['Default_Scale'] = '-3'
df28['Calculation_Type'] = 'Level'
df28['County_FIPS'] = df28['GeoFIPS']
df28['County_Name'] = df28['GeoName']
df28['Measure_Hierarchy_Level'] = '4'


df29 = pd.read_csv('../Updates/STG_BEA_CA5N_Other_Services.txt', sep='\t')
df29['Economic_Measure_Code'] = 'BEA_CA5N_1900'
df29['Published_UOM'] = 'N$'
df29['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df29['Unit_of_Measure_Code'] = 'R$'
df29['Default_Scale'] = '-3'
df29['Calculation_Type'] = 'Level'
df29['County_FIPS'] = df29['GeoFIPS']
df29['County_Name'] = df29['GeoName']
df29['Measure_Hierarchy_Level'] = '4'


df30 = pd.read_csv('../Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt', sep='\t')
df30['Economic_Measure_Code'] = 'BEA_CA5N_2000'
df30['Published_UOM'] = 'N$'
df30['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df30['Unit_of_Measure_Code'] = 'R$'
df30['Default_Scale'] = '-3'
df30['Calculation_Type'] = 'Level'
df30['County_FIPS'] = df30['GeoFIPS']
df30['County_Name'] = df30['GeoName']
df30['Measure_Hierarchy_Level'] = '2'


df31 = pd.read_csv('../Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt', sep='\t')
df31['Economic_Measure_Code'] = 'BEA_CA5N_2001'
df31['Published_UOM'] = 'N$'
df31['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df31['Unit_of_Measure_Code'] = 'R$'
df31['Default_Scale'] = '-3'
df31['Calculation_Type'] = 'Level'
df31['County_FIPS'] = df31['GeoFIPS']
df31['County_Name'] = df31['GeoName']
df31['Measure_Hierarchy_Level'] = '4'


df32 = pd.read_csv('../Updates/STG_BEA_CA5N_Military_Government.txt', sep='\t')
df32['Economic_Measure_Code'] = 'BEA_CA5N_2002'
df32['Published_UOM'] = 'N$'
df32['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df32['Unit_of_Measure_Code'] = 'R$'
df32['Default_Scale'] = '-3'
df32['Calculation_Type'] = 'Level'
df32['County_FIPS'] = df32['GeoFIPS']
df32['County_Name'] = df32['GeoName']
df32['Measure_Hierarchy_Level'] = '4'


df33 = pd.read_csv('../Updates/STG_BEA_CA5N_State_and_Local.txt', sep='\t')
df33['Economic_Measure_Code'] = 'BEA_CA5N_2010'
df33['Published_UOM'] = 'N$'
df33['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df33['Unit_of_Measure_Code'] = 'R$'
df33['Default_Scale'] = '-3'
df33['Calculation_Type'] = 'Level'
df33['County_FIPS'] = df33['GeoFIPS']
df33['County_Name'] = df33['GeoName']
df33['Measure_Hierarchy_Level'] = '4'


df34 = pd.read_csv('../Updates/STG_BEA_CA5N_State_Government.txt', sep='\t')
df34['Economic_Measure_Code'] = 'BEA_CA5N_2011'
df34['Published_UOM'] = 'N$'
df34['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df34['Unit_of_Measure_Code'] = 'R$'
df34['Default_Scale'] = '-3'
df34['Calculation_Type'] = 'Level'
df34['County_FIPS'] = df34['GeoFIPS']
df34['County_Name'] = df34['GeoName']
df34['Measure_Hierarchy_Level'] = '5'

df35 = pd.read_csv('../Updates/STG_BEA_CA5N_Local_Government.txt', sep='\t')
df35['Economic_Measure_Code'] = 'BEA_CA5N_2012'
df35['Published_UOM'] = 'N$'
df35['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df35['Unit_of_Measure_Code'] = 'R$'
df35['Default_Scale'] = '-3'
df35['Calculation_Type'] = 'Level'
df35['County_FIPS'] = df35['GeoFIPS']
df35['County_Name'] = df35['GeoName']
df35['Measure_Hierarchy_Level'] = '5'

df_list = [df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18, df19, df20, df21, df22, df23, df24, df25, df26, df27, df28, df29, df30, df31, df32, df33, df34, df35]

df = df1.append(df_list)

df = df.drop(['Region', 'TableName', 'LineCode', 'IndustryClassification', 'Unit'], axis=1)

df = df.rename(columns = {'Description':'Economic_Measure_Name', 'GeoName':'GeoArea_Name', 'GeoFIPS':'GeoArea_FIPS'})

df = df.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Published_UOM', 'Unit_of_Measure_Code', 'Calculation_Type', 'Default_Scale', 'Estimation_Qualifier', 'Measure_Hierarchy_Level'], var_name='Date', value_name='Published_Value')

df['Estimated_Real_Value'] = df['Published_Value']


df['PZ_Name'] = ''
df['WDB_Name'] = ''
df['Data_Period_Type'] = 'YR'
df['State_FIPS'] = '37000'
df['State_Name'] = 'North Carolina'
df['Region_FIPS'] = '95000'
df['Region_Name'] = 'Southeast'
df['Nation_Name'] = 'USA'
df['Nation_FIPS'] = '00000'
df['GeoFIPS_Type'] = 'CNTY'
df['County_FIPS'] = df['GeoArea_FIPS']
df['County_Name'] = df['GeoArea_Name']

df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df['Data_Period_Business_Key'] = df['Date'].dt.strftime('%Y')
df['Data_Period_Name'] = df['Date'].dt.strftime('The Year %Y')


# North Central
df.loc[df['GeoArea_Name'].str.contains('Chatham|Durham|Edgecombe|Franklin|Granville|Harnett|Johnston|Lee|Nash|Orange|Person|Vance|Wake|Warren|Wilson'), 'PZ_Name'] = 'North Central'

# Northeast
df.loc[df['GeoArea_Name'].str.contains('Beaufort|Bertie|Camden|Chowan|Tyrrell|Hyde|Currituck|Dare|Gates|Halifax|Hertford|Hyde|Martin|Northampton|Pasquotank|Perquimans|Pitt|Washington'), 'PZ_Name'] = 'Northeast'

# Northwest
df.loc[df['GeoArea_Name'].str.contains('Avery|Alleghany|Alexander|Ashe|Burke|Caldwell|Catawba|McDowell|Mitchell|Watauga|Wilkes|Yancey'), 'PZ_Name'] = 'Northwest'

# Piedmont-Triad
df.loc[df['GeoArea_Name'].str.contains('Alamance|Caswell|Davidson|Davie|Forsyth|Guilford|Randolph|Rockingham|Stokes|Surry|Yadkin'), 'PZ_Name'] = 'Piedmont-Triad'

# South Central
df.loc[df['GeoArea_Name'].str.contains('Bladen|Columbus|Cumberland|Hoke|Montgomery|Moore|Richmond|Roberson|Sampson|Scotland'), 'PZ_Name'] = 'South Central'

# Southeast
df.loc[df['GeoArea_Name'].str.contains('Jones|Brunswick|Carteret|Craven|Duplin|Greene|Lenoir|New Hanover|Onslow|Pamlico|Pender|Wayne'), 'PZ_Name'] = 'Southeast'

# Southwest
df.loc[df['GeoArea_Name'].str.contains('Anson|Cabarrus|Cleveland|Gaston|Iredell|Lincoln|Mecklenburg|Rowan|Stanly|Union'), 'PZ_Name'] = 'Southwest'

# Western
df.loc[df['GeoArea_Name'].str.contains('Graham|Buncombe|Cherokee|Clay|Haywood|Henderson|Jackson|Macon|Madison|Polk|Rutherford|Swain|Transylvania'), 'PZ_Name'] = 'Western'

#Cape Fear
df.loc[df['GeoArea_Name'].str.contains('Brunswick|Columbus|New Hanover|Pender'), 'WDB_Name'] = 'Cape Fear'

#Capital Area
df.loc[df['GeoArea_Name'].str.contains('Johnson|Wake'), 'WDB_Name'] = 'Capital Area'

#Centralina
df.loc[df['GeoArea_Name'].str.contains('Anson|Cabarrus|Iredell|Lincoln|Rowan|Stanley|Union'), 'WDB_Name'] = 'Centralina'

#Charlotte Works
df.loc[df['GeoArea_Name'].str.contains('Mecklenburgh'), 'WDB_Name'] = 'Charlotte Works'

#Cumberland County
df.loc[df['GeoArea_Name'].str.contains('Cumberland'), 'WDB_Name'] = 'Cumberland County'

#DavidsonWorks, Inc.
df.loc[df['GeoArea_Name'].str.contains('Davidson'), 'WDB_Name'] = 'DavidsonWorks, Inc.'

#Durham
df.loc[df['GeoArea_Name'].str.contains('Durham'), 'WDB_Name'] = 'Durham'

#Eastern Carolina
df.loc[df['GeoArea_Name'].str.contains('Carteret|Craven|Duplin|Greene|Jones|Lenoir|Onslow|Pamlico|Wayne'), 'WDB_Name'] = 'Eastern Carolina'

#Gaston County
df.loc[df['GeoArea_Name'].str.contains('Gaston'), 'WDB_Name'] = 'Gaston County'

#Greensboro/High Point/Guilford
df.loc[df['GeoArea_Name'].str.contains('Guilford'), 'WDB_Name'] = 'Greensboro/High Point/Guilford'

#High Country
df.loc[df['GeoArea_Name'].str.contains('Alleghany|Ashe|Yancey|Avery|Mitchell|Watauga|Wilkes'), 'WDB_Name'] = 'High Country'

#Kerr-Tar
df.loc[df['GeoArea_Name'].str.contains('Caswell|Franklin|Granville|Person|Vance|Warren'), 'WDB_Name'] = 'Kerr-Tar'

#Lumber River
df.loc[df['GeoArea_Name'].str.contains('Bladen|Hoke|Richmond|Roberson|Scotland'), 'WDB_Name'] = 'Lumber River'

#Mountain Area
df.loc[df['GeoArea_Name'].str.contains('Buncombe|Henderson|Madison|Transylvania'), 'WDB_Name'] = 'Mountain Area'

#Northeastern
df.loc[df['GeoArea_Name'].str.contains('Camden|Chowan|Currituck|Dare|Gates|Hyde|Pasquotank|Perquimans|Washington|Tyrrell'), 'WDB_Name'] = 'Northeastern'

#Northwest Piedmont
df.loc[df['GeoArea_Name'].str.contains('Davie|Forsyth|Rockingham|Stokes|Surry|Yadkin'), 'WDB_Name'] = 'Northwest Piedmont'

#Region C
df.loc[df['GeoArea_Name'].str.contains('Cleveland|McDowell|Polk|Rutherford'), 'WDB_Name'] = 'Region C'

#Region Q
df.loc[df['GeoArea_Name'].str.contains('Beaufort|Bertie|Hertford|Martin|Pitt'), 'WDB_Name'] = 'Region Q'

#Regional Partnership
df.loc[df['GeoArea_Name'].str.contains('Alamance|Montgomery|Moore|Orange|Randolph'), 'WDB_Name'] = 'Regional Partnership'

#Southwestern
df.loc[df['GeoArea_Name'].str.contains('Cherokee|Clay|Graham|Haywood|Macon|Jackson|Swain'), 'WDB_Name'] = 'Southwestern'

#Triangle South
df.loc[df['GeoArea_Name'].str.contains('Chatham|Harnett|Sampson|Lee'), 'WDB_Name'] = 'Triangle South'

#Turning Point
df.loc[df['GeoArea_Name'].str.contains('Edgecombe|Halifax|Nash|Northampton|Wilson'), 'WDB_Name'] = 'Turning Point'

#Western Piedmont
df.loc[df['GeoArea_Name'].str.contains('Alexander|Burke|Caldwell|Catawba'), 'WDB_Name'] = 'Western Piedmont'

df['Data_Period_Begin_Datetime'] = df['Date'].dt.strftime('%m/1/%Y %#H:%M')

columns = ['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Data_Period_Business_Key', 'Published_Value', 'Published_UOM', 'Estimated_Real_Value','Estimation_Qualifier', 'Unit_of_Measure_Code', 'Default_Scale', 'Data_Period_Type', 'Data_Period_Name', 'Data_Period_Begin_Datetime', 'Calculation_Type', 'Measure_Hierarchy_Level', 'GeoFIPS_Type', 'County_FIPS', 'County_Name', 'PZ_Name', 'WDB_Name', 'State_FIPS', 'State_Name', 'Region_FIPS', 'Region_Name', 'Nation_FIPS', 'Nation_Name']

df = df[columns]
df.set_index('GeoArea_FIPS', inplace=True)

df.to_csv('../Updates/STG_WNCD_Earnings_Data_Series.txt', sep='\t')

# Connect to Final_Copy
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=TITANIUM-BOOK;"
    "Database=DataDashboard;"
    "Trusted_Connection=yes;",
    autocommit=True,
)

c = con.cursor()

c.execute("drop table STG_WNCD_Earnings_Data_Series_BACKUP")

c.execute("""sp_rename 'STG_WNCD_Earnings_Data_Series', 'STG_WNCD_Earnings_Data_Series_BACKUP';""")

params = urllib.parse.quote_plus(
    r"Driver={SQL Server};"
    r"Server=TITANIUM-BOOK;"
    r"Database=DataDashboard;"
    r"Trusted_Connection=yes;"
)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df.to_sql(
    "STG_WNCD_Earnings_Data_Series", con=engine, if_exists="replace", index=False
    )