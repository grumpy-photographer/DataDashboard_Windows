import urllib
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc
import datetime as dt


backup_df = pd.read_csv('../Updates/STG_WNCD_Labor_Data_Series.txt', sep='\t')
backup_df.to_csv('../Backups/STG_WNCD_Labor_Data_Series_BACKUP.txt', sep='\t')


#### BEA ####

# personal income
df1 = pd.read_csv('../Updates/STG_BEA_CA5N_Personal_Income.txt', sep='\t')
df1['Economic_Measure_Code'] = 'BEA_CA5N_0010'
df1['Published_UOM'] = 'N$'
df1['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df1['Unit_of_Measure_Code'] = 'R$'
df1['Default_Scale'] = '-3'
df1['Calculation_Type'] = 'Level'

# population
df2 = pd.read_csv('../Updates/STG_BEA_CA5N_Population.txt', sep='\t')
df2['Economic_Measure_Code'] = 'BEA_CA5N_0020'
df2['Published_UOM'] = 'UNIT'
df2['Estimation_Qualifier'] = ''
df2['Unit_of_Measure_Code'] = 'UNIT'
df2['Default_Scale'] = '0'
df2['Calculation_Type'] = 'Level'

# per capita personal income
df3 = pd.read_csv('../Updates/STG_BEA_CA5N_Per_Capita_Personal_Income.txt', sep='\t')
df3['Economic_Measure_Code'] = 'BEA_CA5N_0030'
df3['Published_UOM'] = 'N$'
df3['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df3['Unit_of_Measure_Code'] = 'R$'
df3['Default_Scale'] = '0'
df3['Calculation_Type'] = 'Ratio'

# Earnings by place of work
df4 = pd.read_csv('../Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt', sep='\t')
df4['Economic_Measure_Code'] = 'BEA_CA5N_0035'
df4['Published_UOM'] = 'N$'
df4['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df4['Unit_of_Measure_Code'] = 'R$'
df4['Default_Scale'] = '-3'
df4['Calculation_Type'] = 'Level'

# Average Compensation
df5 = pd.read_csv('../Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt', sep='\t')
df5['Economic_Measure_Code'] = 'BEA_CA6N_0009'
df5['Published_UOM'] = 'N$'
df5['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
df5['Unit_of_Measure_Code'] = 'R$'
df5['Default_Scale'] = '0'
df5['Calculation_Type'] = 'Ratio'

# CA6N
files = {
    'BEA_CA6N_0001': "../Updates/STG_BEA_CA6N_Compensation_of_Employees.txt",
    'BEA_CA6N_0005': "../Updates/STG_BEA_CA6N_Wages_and_Salaries.txt",
    'BEA_CA6N_0006': "../Updates/STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt",
    'BEA_CA6N_0007': "../Updates/STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt",
    'BEA_CA6N_0008': "../Updates/STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt",
    'BEA_CA6N_0081': "../Updates/STG_BEA_CA6N_Farm_Compensation.txt",
    'BEA_CA6N_0082': "../Updates/STG_BEA_CA6N_NonFarm_Compensation.txt",
    'BEA_CA6N_090': "../Updates/STG_BEA_CA6N_Private_Nonfarm_Compensation.txt",
    'BEA_CA6N_0100': "../Updates/STG_BEA_CA6N_Average_Compensation_Per_Job.txt",
    'BEA_CA6N_0200': "../Updates/STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt",
    'BEA_CA6N_0300': "../Updates/STG_BEA_CA6N_Utilities.txt",
    'BEA_CA6N_0400': "../Updates/STG_BEA_CA6N_Construction.txt",
    'BEA_CA6N_0500': "../Updates/STG_BEA_CA6N_Manufacturing.txt",
    'BEA_CA6N_0600': "../Updates/STG_BEA_CA6N_Wholesale_Trade.txt",
    'BEA_CA6N_0700': "../Updates/STG_BEA_CA6N_Retail_Trade.txt",
    'BEA_CA6N_0800': "../Updates/STG_BEA_CA6N_Transportation_and_Warehousing.txt",
    'BEA_CA6N_0900': "../Updates/STG_BEA_CA6N_Information.txt",
    'BEA_CA6N_1000': "../Updates/STG_BEA_CA6N_Finance_and_Insurance.txt",
    'BEA_CA6N_1100': "../Updates/STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt",
    'BEA_CA6N_1200': "../Updates/STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt",
    'BEA_CA6N_1300': "../Updates/STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt",
    'BEA_CA6N_1400': "../Updates/STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    'BEA_CA6N_1500': "../Updates/STG_BEA_CA6N_Educational_Services.txt",
    'BEA_CA6N_1600': "../Updates/STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt",
    'BEA_CA6N_1700': "../Updates/STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt",
    'BEA_CA6N_1800': "../Updates/STG_BEA_CA6N_Accommodation_and_Food_Services.txt",
    'BEA_CA6N_1900': "../Updates/STG_BEA_CA6N_Other_Services.txt",
    'BEA_CA6N_2000': "../Updates/STG_BEA_CA6N_Government_and_Government_Enterprises.txt"
    }

for key, value in files.items():
    df6 = pd.read_csv(value, sep='\t')
    df6['Economic_Measure_Code'] = key
    df6['Published_UOM'] = 'N$'
    df6['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
    df6['Unit_of_Measure_Code'] = 'R$'
    df6['Default_Scale'] = '-3'
    df6['Calculation_Type'] = 'Level'
    
df_list = [df1, df2, df3, df4, df5, df6]
df_bea = df1.append(df_list)

df_bea = df_bea.drop(['Region', 'TableName', 'LineCode', 'IndustryClassification', 'Unit'], axis=1)

df_bea = df_bea.rename(columns = {'Description':'Economic_Measure_Name', 'GeoName':'GeoArea_Name', 'GeoFIPS':'GeoArea_FIPS'})

df_bea = df_bea.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Published_UOM', 'Unit_of_Measure_Code', 'Calculation_Type', 'Default_Scale', 'Estimation_Qualifier'], var_name='Date', value_name='Published_Value')

df_bea['Estimated_Real_Value'] = df_bea['Published_Value']


df_bea['PZ_Name'] = ''
df_bea['WDB_Name'] = ''
df_bea['Data_Period_Type'] = 'YR'

df_bea['Date'] = pd.to_datetime(df_bea['Date'], errors='coerce')
df_bea['Data_Period_Business_Key'] = df_bea['Date'].dt.strftime('%Y')
df_bea['Data_Period_Name'] = df_bea['Date'].dt.strftime('The Year %Y')






#### FRED ####

# Unemployment Rate by County
df7 = pd.read_csv('../Updates/STG_FRED_Unemployment_Rate_by_County_Percent.txt', sep='\t')
df7['Economic_Measure_Code'] = 'FRED_LAUCN_003A'
df7['Economic_Measure_Name'] = 'Unemployment Rate By County (Percent)'
df7['Published_UOM'] = 'PCT'
df7['Unit_of_Measure_Code'] = 'PCT'
df7['Calculation_Type'] = 'Percent'
df7['Default_Scale'] = '2'
df7['Estimation_Qualifier'] = ''

df7 = df7.rename(columns = {'Region Name':'GeoArea_Name', 'Region Code':'GeoArea_FIPS'})

df7 = df7.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Published_UOM', 'Unit_of_Measure_Code', 'Calculation_Type', 'Default_Scale', 'Estimation_Qualifier'], var_name='Date', value_name='Published_Value')

df7['Estimated_Real_Value'] = df7['Published_Value']


# Civilian Labor Force
df8 = pd.read_csv('../Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt', sep='\t')
df8['Economic_Measure_Code'] = 'FRED_LFN_00000'
df8['Economic_Measure_Name'] = 'Civilian Labor Force by County'
df8['Published_UOM'] = 'PRS'
df8['Unit_of_Measure_Code'] = 'PRS'
df8['Calculation_Type'] = 'Level'
df8['Default_Scale'] = '0'
df8['Estimation_Qualifier'] = ''

df8= df8.rename(columns = {'Region Name':'GeoArea_Name', 'Region Code':'GeoArea_FIPS'})

df8 = df8.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Published_UOM', 'Unit_of_Measure_Code', 'Calculation_Type', 'Default_Scale', 'Estimation_Qualifier'], var_name='Date', value_name='Published_Value')

df8['Estimated_Real_Value'] = df8['Published_Value']


# Resident Population
df9 = pd.read_csv('../Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt', sep='\t')
df9['Economic_Measure_Code'] = 'FRED_POP_00000'
df9['Economic_Measure_Name'] = 'Resident Population by County'
df9['Published_UOM'] = 'PRS'
df9['Unit_of_Measure_Code'] = 'PRS'
df9['Calculation_Type'] = 'Level'
df9['Default_Scale'] = '-3'
df9['Estimation_Qualifier'] = ''

df9 = df9.rename(columns = {'Region Name':'GeoArea_Name', 'Region Code':'GeoArea_FIPS'})

df9 = df9.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Published_UOM', 'Unit_of_Measure_Code', 'Calculation_Type', 'Default_Scale', 'Estimation_Qualifier'], var_name='Date', value_name='Published_Value')

df9['Estimated_Real_Value'] = df9['Published_Value']


# People 25 Years Education
df10 = pd.read_csv('../Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt', sep='\t')
df10['Economic_Measure_Code'] = 'FRED_S1501ACSTOTAL0_00000'
df10['Economic_Measure_Name'] = "People 25 Years and Over Who Have Completed an Associate's Degree or Higher (5-year estimate) by County (Percent)"
df10['Published_UOM'] = 'PCT'
df10['Unit_of_Measure_Code'] = 'PCT'
df10['Calculation_Type'] = 'Percent'
df10['Default_Scale'] = '2'
df10['Estimation_Qualifier'] = ''

df10 = df10.rename(columns = {'Region Name':'GeoArea_Name', 'Region Code':'GeoArea_FIPS'})

df10 = df10.melt(id_vars=['GeoArea_FIPS', 'GeoArea_Name', 'Economic_Measure_Code', 'Economic_Measure_Name', 'Published_UOM', 'Unit_of_Measure_Code', 'Calculation_Type', 'Default_Scale', 'Estimation_Qualifier'], var_name='Date', value_name='Published_Value')

df10['Estimated_Real_Value'] = df10['Published_Value']


df_fred_list = [df7, df8, df9, df10]
df_fred = df7.append(df_fred_list)

column_list = df_fred.columns.values
for i in column_list:
    df_fred.loc[df_fred[i].isnull(), i] = 0

df_fred['PZ_Name'] = ''
df_fred['WDB_Name'] = ''
df_fred['Data_Period_Type'] = 'YR'

df_fred['Date'] = pd.to_datetime(df_fred['Date'], errors='coerce')
df_fred['Data_Period_Business_Key'] = df_fred['Date'].dt.strftime('%Y')
df_fred['Data_Period_Name'] = df_fred['Date'].dt.strftime('The Year %Y')






#### Final Copy ####
df = df_bea.append(df_fred)

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

#Cumberlabor County
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

columns = ['GeoArea_FIPS', 'GeoArea_Name','Economic_Measure_Code', 'Economic_Measure_Name', 'Data_Period_Business_Key','Published_Value', 'Published_UOM', 'Estimated_Real_Value', 'Estimation_Qualifier', 'Default_Scale', 'Data_Period_Type','Data_Period_Name','Data_Period_Begin_Datetime', 'Calculation_Type', 'PZ_Name', 'WDB_Name']

df = df[columns]
df.set_index('GeoArea_FIPS', inplace=True)

df.to_csv('../Updates/STG_WNCD_Labor_Data_Series.txt', sep='\t')

# Connect to Final_Copy
con = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=TITANIUM-BOOK;"
    "Database=DataDashboard;"
    "Trusted_Connection=yes;",
    autocommit=True,
)

c = con.cursor()

c.execute("drop table STG_WNCD_Labor_Data_Series_BACKUP")

c.execute("""sp_rename 'STG_WNCD_Labor_Data_Series', 'STG_WNCD_Labor_Data_Series_BACKUP';""")

params = urllib.parse.quote_plus(
    r"Driver={SQL Server};"
    r"Server=TITANIUM-BOOK;"
    r"Database=DataDashboard;"
    r"Trusted_Connection=yes;"
)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df.to_sql(
    "STG_WNCD_Labor_Data_Series", con=engine, if_exists="replace", index=False
    )