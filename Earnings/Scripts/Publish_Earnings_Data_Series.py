import urllib
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import pyodbc
import datetime as dt


backup_df = pd.read_csv('../Updates/STG_WNCD_Earnings_Data_Series.txt', sep='\t')
backup_df.to_csv('../Backups/STG_WNCD_Earnings_Data_Series_BACKUP.txt', sep='\t')


#### BEA ####

# Measure Hierarchy Level 1
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


files1 = {
    'BEA_CA5N_0010': "../Updates/STG_BEA_CA5N_Personal_Income.txt",
    'BEA_CA5N_0035': "../Updates/STG_BEA_CA5N_Earnings_by_Place_of_Work.txt"
    }

for key, value in files1.items():
    df3 = pd.read_csv(value, sep='\t')
    df3['Economic_Measure_Code'] = key
    df3['Published_UOM'] = 'N$'
    df3['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
    df3['Unit_of_Measure_Code'] = 'R$'
    df3['Default_Scale'] = '-3'
    df3['Calculation_Type'] = 'Level'
    df3['County_FIPS'] = df3['GeoFIPS']
    df3['County_Name'] = df3['GeoName']
    df3['Measure_Hierarchy_Level'] = '1'
    df_append1 = df3.append(df2)
    pass


# Measure Hierarchy Level 2
files2 = {
    'BEA_CA5N_0050': "../Updates/STG_BEA_CA5N_Wages_and_Salaries.txt",
    'BEA_CA5N_0060': "../Updates/STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt",
    'BEA_CA5N_0070': "../Updates/STG_BEA_CA5N_Proprietors_Income.txt",
    'BEA_CA5N_0081': "../Updates/STG_BEA_CA5N_Farm_Earnings.txt",
    'BEA_CA5N_0082': "../Updates/STG_BEA_CA5N_Private_Nonfarm_Earnings.txt"
    }

for key, value in files2.items():
    df4 = pd.read_csv(value, sep='\t')
    df4['Economic_Measure_Code'] = key
    df4['Published_UOM'] = 'N$'
    df4['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
    df4['Unit_of_Measure_Code'] = 'R$'
    df4['Default_Scale'] = '-3'
    df4['Calculation_Type'] = 'Level'
    df4['County_FIPS'] = df4['GeoFIPS']
    df4['County_Name'] = df4['GeoName']
    df4['Measure_Hierarchy_Level'] = '2'
    df_append2 = df_append1.append(df4)
    pass

print(df_append2)

'''# Measure Hierarchy Level 3
files3 = {
    'BEA_CA5N_0090': "../Updates/STG_BEA_CA5N_Private_Nonfarm_Earnings.txt",
    'BEA_CA5N_2000': "../Updates/STG_BEA_CA5N_Government_and_Government_Enterprises.txt"
    }

for key, value in files3.items():
    df5 = pd.read_csv(value, sep='\t')
    df5['Economic_Measure_Code'] = key
    df5['Published_UOM'] = 'N$'
    df5['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
    df5['Unit_of_Measure_Code'] = 'R$'
    df5['Default_Scale'] = '-3'
    df5['Calculation_Type'] = 'Level'
    df5['County_FIPS'] = df5['GeoFIPS']
    df5['County_Name'] = df5['GeoName']
    df5['Measure_Hierarchy_Level'] = '3'
    pass


# Measure Hierarchy Level 4
files4 = {
    'BEA_CA5N_0100': "../Updates/STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt",
    'BEA_CA5N_0200': "../Updates/STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt",
    'BEA_CA5N_0300': "../Updates/STG_BEA_CA5N_Utilities.txt",
    'BEA_CA5N_0400': "../Updates/STG_BEA_CA5N_Construction.txt",
    'BEA_CA5N_0500': "../Updates/STG_BEA_CA5N_Manufacturing.txt",
    'BEA_CA5N_0600': "../Updates/STG_BEA_CA5N_Wholesale_Trade.txt",
    'BEA_CA5N_0700': "../Updates/STG_BEA_CA5N_Retail_Trade.txt",
    'BEA_CA5N_0800': "../Updates/STG_BEA_CA5N_Transportation_and_Warehousing.txt",
    'BEA_CA5N_0900': "../Updates/STG_BEA_CA5N_Information.txt",
    'BEA_CA5N_1000': "../Updates/STG_BEA_CA5N_Finance_and_Insurance.txt",
    'BEA_CA5N_1100': "../Updates/STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt",
    'BEA_CA5N_1200': "../Updates/STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt",
    'BEA_CA5N_1300': "../Updates/STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt",
    'BEA_CA5N_1400': "../Updates/STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt",
    'BEA_CA5N_1500': "../Updates/STG_BEA_CA5N_Educational_Services.txt",
    'BEA_CA5N_1600': "../Updates/STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt",
    'BEA_CA5N_1700': "../Updates/STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt",
    'BEA_CA5N_1800': "../Updates/STG_BEA_CA5N_Accommodation_and_Food_Services.txt",
    'BEA_CA5N_1900': "../Updates/STG_BEA_CA5N_Other_Services.txt",
    'BEA_CA5N_2001': "../Updates/STG_BEA_CA5N_Federal_Civilian_Government.txt",
    'BEA_CA5N_2002': "../Updates/STG_BEA_CA5N_Military_Government.txt",
    'BEA_CA5N_2010': "../Updates/STG_BEA_CA5N_State_and_Local.txt"
    }

for key, value in files4.items():
    df6 = pd.read_csv(value, sep='\t')
    df6['Economic_Measure_Code'] = key
    df6['Published_UOM'] = 'N$'
    df6['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
    df6['Unit_of_Measure_Code'] = 'R$'
    df6['Default_Scale'] = '-3'
    df6['Calculation_Type'] = 'Level'
    df6['County_FIPS'] = df6['GeoFIPS']
    df6['County_Name'] = df6['GeoName']
    df6['Measure_Hierarchy_Level'] = '4'
    pass


# Measure Hierarchy Level 5
files5 = {
    'BEA_CA5N_2011': "../Updates/STG_BEA_CA5N_State_Government.txt",
    'BEA_CA5N_2012': "../Updates/STG_BEA_CA5N_Local_Government.txt"
    }

for key, value in files5.items():
    df7 = pd.read_csv(value, sep='\t')
    df7['Economic_Measure_Code'] = key
    df7['Published_UOM'] = 'N$'
    df7['Estimation_Qualifier'] = 'Real 2012 Chained Dollars'
    df7['Unit_of_Measure_Code'] = 'R$'
    df7['Default_Scale'] = '-3'
    df7['Calculation_Type'] = 'Level'
    df7['County_FIPS'] = df7['GeoFIPS']
    df7['County_Name'] = df7['GeoName']
    df7['Measure_Hierarchy_Level'] = '5'
    pass
    

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
    )'''