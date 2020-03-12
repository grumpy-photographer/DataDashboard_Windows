#!/usr/bin/env python -W ignore

#Imports
import pandas as pd
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np
import os

print('----------------------------------------------\nNathan Young\nJunior Data Analyst, Lead Programmer\nNC Data Dashboard\nCenter for the Study of Free Enterprise\nWestern Carolina University\nLast Updated: Mar 10 2020')

try:
    #Run Program
    def runProgram(): #done
        os.chdir('C:/Users/natha/OneDrive/Desktop/GitHub/DataDashboard_Windows')
        folder = int(input('----------------------------------------------\nWhich folder would you like to clean data for?\nType 0 for list of options. '))
        if folder == 0:
            print('1-Demographics\n2-Earnings\n3-Health.\n4-Labor\n5-Land\n6-Natural Products\n999-Exit')
            runProgram()
        elif folder == 1:
            print('Taking you to Demographics...')
            os.chdir('./Demographics')
            demographics_update()
        elif folder == 2:
            print('Taking you to Earnings...')
            os.chdir('./Earnings')
            earnings_update()     
        elif folder == 3:
            print('Taking you to Health...')
            os.chdir('./Health')
            health_update()
        elif folder == 4:
            print('Taking you to Labor...')
            os.chdir('./Labor')
            labor_update()
        elif folder == 5:
            print('Taking you to Land...')
            os.chdir('./Land')
            land_update()
        elif folder == 6:
            print('Taking you to Natural Products...')
            os.chdir('./Natural Products')
            natproducts_update()
        elif folder == 999:
            print('Have a nice day!')
            exit()
        else:
            print('Something went wrong.  Please try again.')

        while True:
            end = int(input('Would you like to run the program again: 1-Yes or 2-No?'))
            if end == 1:
                runProgram()
            elif end == 2:
                print('Thanks!')
                exit()
            else:
                print('Please enter 1 for Yes or 2 for No.')

    #Leave Program
    def endProgram(): #done
        answer = int(input('\n----------------------------------------------\nWould you like to update the folder again? 1-Yes or 2-No.\n'))
        if answer == 1:
                demographics_update()
                earnings_update()
                health_update()
                labor_update()
                land_update()  
                natproducts_update()         
        elif answer == 2:
                answer2 = int(input('Return to main menu? 1-Yes or 2-No.\n'))
                if answer2 == 1:
                    print('Returning to main menu.')
                    runProgram()
                    pass
                elif answer2 == 2:
                    print('Thanks! Have a nice day!')
                    exit()
        
        while True:
            runProgram()
            exit()

    #Publish to Database
    def SQL(): #working
        print('working')
        '''df = pd.read(filename)
        tablename = 'STG_',filename
        con = pyodbc.connect('Driver={SQL Server};'
                            'Server=TITANIUM-BOOK;'
                            'Database=DataDashboard;'
                            'Trusted_Connection=yes;',
                            autocommit=True)
        c = con.cursor()
        c.execute('drop table STG_ZLLW_County_Zhvi_AllHomes_BACKUP')
        c.execute(sp_rename 'dbo.STG_ZLLW_County_Zhvi_AllHomes','STG_ZLLW_County_Zhvi_AllHomes_BACKUP';)
        c.execute("""USE [DataDashboard]
        SET ANSI_NULLS ON
        SET QUOTED_IDENTIFIER ON
        CREATE TABLE [dbo].['tablename'](
        [RegionName] [varchar](19) NULL,
        [RegionID] [smallint] NULL,
        [State] [varchar](2) NULL,
        [Metro] [varchar](38) NULL,
        [StateCodeFIPS] [varchar](2) NULL,
        [MunicipalCodeFIPS] [varchar](3) NULL,
        [SizeRank] [smallint] NULL,
        [1996-04] [float] NULL,
        [1996-05] [float] NULL,
        [1996-06] [float] NULL,
        [1996-07] [float] NULL,
        [1996-08] [float] NULL,
        [1996-09] [float] NULL,
        [1996-10] [float] NULL,
        [1996-11] [float] NULL,
        [1996-12] [float] NULL,
        [1997-01] [float] NULL,
        [1997-02] [float] NULL,
        [1997-03] [float] NULL,
        [1997-04] [float] NULL,
        [1997-05] [float] NULL,
        [1997-06] [float] NULL,
        [1997-07] [float] NULL,
        [1997-08] [float] NULL,
        [1997-09] [float] NULL,
        [1997-10] [float] NULL,
        [1997-11] [float] NULL,
        [1997-12] [float] NULL,
        [1998-01] [float] NULL,
        [1998-02] [float] NULL,
        [1998-03] [float] NULL,
        [1998-04] [float] NULL,
        [1998-05] [float] NULL,
        [1998-06] [float] NULL,
        [1998-07] [float] NULL,
        [1998-08] [float] NULL,
        [1998-09] [float] NULL,
        [1998-10] [float] NULL,
        [1998-11] [float] NULL,
        [1998-12] [float] NULL,
        [1999-01] [float] NULL,
        [1999-02] [float] NULL,
        [1999-03] [float] NULL,
        [1999-04] [float] NULL,
        [1999-05] [float] NULL,
        [1999-06] [float] NULL,
        [1999-07] [float] NULL,
        [1999-08] [float] NULL,
        [1999-09] [float] NULL,
        [1999-10] [float] NULL,
        [1999-11] [float] NULL,
        [1999-12] [float] NULL,
        [2000-01] [float] NULL,
        [2000-02] [float] NULL,
        [2000-03] [float] NULL,
        [2000-04] [float] NULL,
        [2000-05] [float] NULL,
        [2000-06] [float] NULL,
        [2000-07] [float] NULL,
        [2000-08] [float] NULL,
        [2000-09] [float] NULL,
        [2000-10] [float] NULL,
        [2000-11] [float] NULL,
        [2000-12] [float] NULL,
        [2001-01] [float] NULL,
        [2001-02] [float] NULL,
        [2001-03] [float] NULL,
        [2001-04] [float] NULL,
        [2001-05] [float] NULL,
        [2001-06] [float] NULL,
        [2001-07] [float] NULL,
        [2001-08] [float] NULL,
        [2001-09] [float] NULL,
        [2001-10] [float] NULL,
        [2001-11] [float] NULL,
        [2001-12] [float] NULL,
        [2002-01] [float] NULL,
        [2002-02] [float] NULL,
        [2002-03] [float] NULL,
        [2002-04] [float] NULL,
        [2002-05] [float] NULL,
        [2002-06] [float] NULL,
        [2002-07] [float] NULL,
        [2002-08] [float] NULL,
        [2002-09] [float] NULL,
        [2002-10] [float] NULL,
        [2002-11] [float] NULL,
        [2002-12] [float] NULL,
        [2003-01] [float] NULL,
        [2003-02] [float] NULL,
        [2003-03] [float] NULL,
        [2003-04] [float] NULL,
        [2003-05] [float] NULL,
        [2003-06] [float] NULL,
        [2003-07] [float] NULL,
        [2003-08] [float] NULL,
        [2003-09] [float] NULL,
        [2003-10] [float] NULL,
        [2003-11] [float] NULL,
        [2003-12] [float] NULL,
        [2004-01] [float] NULL,
        [2004-02] [float] NULL,
        [2004-03] [float] NULL,
        [2004-04] [float] NULL,
        [2004-05] [float] NULL,
        [2004-06] [float] NULL,
        [2004-07] [float] NULL,
        [2004-08] [float] NULL,
        [2004-09] [float] NULL,
        [2004-10] [float] NULL,
        [2004-11] [float] NULL,
        [2004-12] [float] NULL,
        [2005-01] [float] NULL,
        [2005-02] [float] NULL,
        [2005-03] [float] NULL,
        [2005-04] [float] NULL,
        [2005-05] [float] NULL,
        [2005-06] [float] NULL,
        [2005-07] [float] NULL,
        [2005-08] [float] NULL,
        [2005-09] [float] NULL,
        [2005-10] [float] NULL,
        [2005-11] [float] NULL,
        [2005-12] [float] NULL,
        [2006-01] [float] NULL,
        [2006-02] [float] NULL,
        [2006-03] [float] NULL,
        [2006-04] [float] NULL,
        [2006-05] [float] NULL,
        [2006-06] [float] NULL,
        [2006-07] [float] NULL,
        [2006-08] [float] NULL,
        [2006-09] [float] NULL,
        [2006-10] [float] NULL,
        [2006-11] [float] NULL,
        [2006-12] [float] NULL,
        [2007-01] [float] NULL,
        [2007-02] [float] NULL,
        [2007-03] [float] NULL,
        [2007-04] [float] NULL,
        [2007-05] [float] NULL,
        [2007-06] [float] NULL,
        [2007-07] [float] NULL,
        [2007-08] [float] NULL,
        [2007-09] [float] NULL,
        [2007-10] [float] NULL,
        [2007-11] [float] NULL,
        [2007-12] [float] NULL,
        [2008-01] [float] NULL,
        [2008-02] [float] NULL,
        [2008-03] [float] NULL,
        [2008-04] [float] NULL,
        [2008-05] [float] NULL,
        [2008-06] [float] NULL,
        [2008-07] [float] NULL,
        [2008-08] [float] NULL,
        [2008-09] [float] NULL,
        [2008-10] [float] NULL,
        [2008-11] [float] NULL,
        [2008-12] [float] NULL,
        [2009-01] [float] NULL,
        [2009-02] [float] NULL,
        [2009-03] [float] NULL,
        [2009-04] [float] NULL,
        [2009-05] [float] NULL,
        [2009-06] [float] NULL,
        [2009-07] [float] NULL,
        [2009-08] [float] NULL,
        [2009-09] [float] NULL,
        [2009-10] [float] NULL,
        [2009-11] [float] NULL,
        [2009-12] [float] NULL,
        [2010-01] [float] NULL,
        [2010-02] [float] NULL,
        [2010-03] [float] NULL,
        [2010-04] [float] NULL,
        [2010-05] [float] NULL,
        [2010-06] [float] NULL,
        [2010-07] [float] NULL,
        [2010-08] [float] NULL,
        [2010-09] [float] NULL,
        [2010-10] [float] NULL,
        [2010-11] [float] NULL,
        [2010-12] [float] NULL,
        [2011-01] [float] NULL,
        [2011-02] [float] NULL,
        [2011-03] [float] NULL,
        [2011-04] [float] NULL,
        [2011-05] [float] NULL,
        [2011-06] [float] NULL,
        [2011-07] [float] NULL,
        [2011-08] [float] NULL,
        [2011-09] [float] NULL,
        [2011-10] [float] NULL,
        [2011-11] [float] NULL,
        [2011-12] [float] NULL,
        [2012-01] [float] NULL,
        [2012-02] [float] NULL,
        [2012-03] [float] NULL,
        [2012-04] [float] NULL,
        [2012-05] [float] NULL,
        [2012-06] [float] NULL,
        [2012-07] [float] NULL,
        [2012-08] [float] NULL,
        [2012-09] [float] NULL,
        [2012-10] [float] NULL,
        [2012-11] [float] NULL,
        [2012-12] [float] NULL,
        [2013-01] [float] NULL,
        [2013-02] [float] NULL,
        [2013-03] [float] NULL,
        [2013-04] [float] NULL,
        [2013-05] [float] NULL,
        [2013-06] [float] NULL,
        [2013-07] [float] NULL,
        [2013-08] [float] NULL,
        [2013-09] [float] NULL,
        [2013-10] [float] NULL,
        [2013-11] [float] NULL,
        [2013-12] [float] NULL,
        [2014-01] [float] NULL,
        [2014-02] [float] NULL,
        [2014-03] [float] NULL,
        [2014-04] [float] NULL,
        [2014-05] [float] NULL,
        [2014-06] [float] NULL,
        [2014-07] [float] NULL,
        [2014-08] [float] NULL,
        [2014-09] [float] NULL,
        [2014-10] [float] NULL,
        [2014-11] [float] NULL,
        [2014-12] [float] NULL,
        [2015-01] [float] NULL,
        [2015-02] [float] NULL,
        [2015-03] [float] NULL,
        [2015-04] [float] NULL,
        [2015-05] [float] NULL,
        [2015-06] [float] NULL,
        [2015-07] [float] NULL,
        [2015-08] [float] NULL,
        [2015-09] [float] NULL,
        [2015-10] [float] NULL,
        [2015-11] [float] NULL,
        [2015-12] [float] NULL,
        [2016-01] [float] NULL,
        [2016-02] [float] NULL,
        [2016-03] [float] NULL,
        [2016-04] [float] NULL,
        [2016-05] [float] NULL,
        [2016-06] [float] NULL,
        [2016-07] [float] NULL,
        [2016-08] [float] NULL,
        [2016-09] [float] NULL,
        [2016-10] [float] NULL,
        [2016-11] [float] NULL,
        [2016-12] [float] NULL,
        [2017-01] [float] NULL,
        [2017-02] [float] NULL,
        [2017-03] [float] NULL,
        [2017-04] [float] NULL,
        [2017-05] [float] NULL,
        [2017-06] [float] NULL,
        [2017-07] [float] NULL,
        [2017-08] [float] NULL,
        [2017-09] [float] NULL,
        [2017-10] [float] NULL,
        [2017-11] [float] NULL,
        [2017-12] [float] NULL,
        [2018-01] [float] NULL,
        [2018-02] [float] NULL,
        [2018-03] [float] NULL,
        [2018-04] [float] NULL,
        [2018-05] [float] NULL,
        [2018-06] [float] NULL,
        [2018-07] [float] NULL,
        [2018-08] [float] NULL,
        [2018-09] [float] NULL,
        [2018-10] [float] NULL,
        [2018-11] [float] NULL,
        [2018-12] [float] NULL,
        [2019-01] [float] NULL,
        [2019-02] [float] NULL,
        [2019-03] [float] NULL,
        [2019-04] [float] NULL,
        [2019-05] [float] NULL,
        [2019-06] [float] NULL,
        [2019-07] [float] NULL,
        [2019-08] [float] NULL,
        [2019-09] [float] NULL,
        [2019-10] [float] NULL,
        [2019-11] [float] NULL,
        [2019-12] [float] NULL,
        [2020-01] [float] NULL,
        [2020-02] [float] NULL,
        [2020-03] [float] NULL,
        [2020-04] [float] NULL,
        [2020-05] [float] NULL,
        [2020-06] [float] NULL,
        [2020-07] [float] NULL,
        [2020-08] [float] NULL,
        [2020-09] [float] NULL,
        [2020-10] [float] NULL,
        [2020-11] [float] NULL,
        [2020-12] [float] NULL,
        [2021-01] [float] NULL,
        [2021-02] [float] NULL,
        [2021-03] [float] NULL,
        [2021-04] [float] NULL,
        [2021-05] [float] NULL,
        [2021-06] [float] NULL,
        [2021-07] [float] NULL,
        [2021-08] [float] NULL,
        [2021-09] [float] NULL,
        [2021-10] [float] NULL,
        [2021-11] [float] NULL,
        [2021-12] [float] NULL,
        [2022-01] [float] NULL,
        [2022-02] [float] NULL,
        [2022-03] [float] NULL,
        [2022-04] [float] NULL,
        [2022-05] [float] NULL,
        [2022-06] [float] NULL,
        [2022-07] [float] NULL,
        [2022-08] [float] NULL,
        [2022-09] [float] NULL,
        [2022-10] [float] NULL,
        [2022-11] [float] NULL,
        [2022-12] [float] NULL) ON [PRIMARY]""")
        params = urllib.parse.quote_plus(r'Driver={SQL Server};' r'Server=TITANIUM-BOOK;' r'Database=DataDashboard;' r'Trusted_Connection=yes;')
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df.to_sql(filename, con=engine, if_exists='replace', index=False)'''

        while True:
            endProgram()

    #Clean Census data
    def CNSUS(): #working
        source = int(input('What source are you updating? Type 0 for list of sources. '))
        if source == 1:   
            print('Updating PEPAGESEX USA')
            df = pd.read_csv('./Data/PEP_2018_PEPAGESEX_with_ann_us.csv', skiprows=1)
            df = df.melt(id_vars=['Geography'], var_name='Economic Measure Name', value_name='Estimated Value')
            df = df.drop(df.index[:2])
            print(df.head())
            pass
        elif source == 2:
            print('Updating PEPAGESEX NC')
            df = pd.read_csv('./Data/PEP_2018_PEPAGESEX_with_ann_nc.csv', skiprows=1)
            df = df.melt(id_vars=['Geography'], var_name='Economic Measure Name', value_name='Estimated Value')
            state_filter = df['Geography'].str.contains('North Carolina')
            df = df[state_filter]
            df = df.drop(df.index[:2])
            print(df.head())
            pass
        elif source == 3:
            print('Updating PEPSR6H NC')
            df = pd.read_csv('./Data/PEP_2018_PEPSR6H_with_ann_nc.csv', skiprows=1)
            state_filter = df['Geography'].str.contains('North Carolina')
            df = df[state_filter]
            print(df.head())
            pass
        elif source == 4:
            print('Updating PEPAGESEX County')
            df = pd.read_csv('./Data/PEP_2018_PEPAGESEX_with_ann_county.csv', skiprows=1)
            df = df.melt(id_vars=['Geography'], var_name='Economic Measure Name', value_name='Estimated Value')
            state_filter = df['Geography'].str.contains('North Carolina')
            df = df[state_filter]
            df = df.drop(df.index[:200])
            print(df.head())
            pass
        elif source == 5:
            print('Updating PEPSR6H County')
            df = pd.read_csv('./Data/PEP_2018_PEPSR6H_with_ann_county.csv', skiprows=1)
            state_filter = df['Geography'].str.contains('North Carolina')
            df = df[state_filter]
            print(df.head())
            pass
        else:
            print('Please enter a numeric value for source.')

        while True:
            endProgram()

    #Clean GeoFred data
    def FRED(): #done
        files = {1:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=39.98&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false', 2:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147149&attributes=Not+Seasonally+Adjusted%2C+Quarterly%2C+Percent&aggregationFrequency=Quarterly&aggregationType=Average&transformation=lin&date=2025-01-01&type=xls&startDate=1999-01-01&endDate=2025-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 3:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147063&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Percent&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 4:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.78&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1970-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=582&hideLegend=false', 5:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=942&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Index+2000%3D100&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1975-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1249&hideLegend=false', 6:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=157125&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Rate&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false', 7:'https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.81&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=155206&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Units&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=521&hideLegend=false'}
        source = int(input('What source are you updating? Type 0 for list of sources. '))
        if source == 1:
            print('Updating Civilian Labor Source')
            filename = './Updates/STG_FRED_Civilian_Labor_Force_by_County_Persons.txt'
            backup_fn = './Backups/STG_FRED_Civilian_Labor_Force_by_County_Persons_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 2:
            print('Updating EQFXSUBPRIME')
            filename = './Updates/STG_FRED_EQFXSUBPRIME.txt'
            backup_fn = './Backups/STG_FRED_EQFXSUBPRIME_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 3:
            print('Updating People Under 25 Education Status')
            filename = './Updates/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.txt'
            backup_fn = './Backups/STG_FRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 4:
            print('Updating Resident Population')
            filename = './Updates/STG_FRED_Resident_Population_by_County_Thousands_of_Persons.txt'
            backup_fn = './Backups/STG_FRED_Resident_Population_by_County_Thousands_of_Persons_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 5:
            print('Updating All Transactions House Price Index')
            filename = './Updates/STG_FRED_All_Transactions_House_Price_Index.txt'
            backup_fn = './Backups/STG_FRED_All_Transactions_House_Price_Index_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 6:
            print('Updating Homeownership Rate')
            filename = './Updates/STG_FRED_Homeownership_Rate_by_County.txt'
            backup_fn = './Backups/STG_FRED_Homeownership_Rate_by_County_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 7:
            print('Updating New Private Housing')
            filename = './Updates/STG_FRED_New_Private_Housing_Structures.txt'
            backup_fn = './Backups/STG_FRED_New_Private_Housing_Structures_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            key = source
            for key, value in files.items():
                df = pd.read_excel(value, skiprows=1)
                region_abbr_filter = df['Region Name'].str.contains(', NC')
                df_nc = df[region_abbr_filter]
                df_nc.set_index(df_nc['Series ID'], inplace = True)
                df_nc.drop('Series ID', axis = 1, inplace = True)
                df_nc.to_csv(filename, sep = '\t')
                pass
        elif source == 0:
            print('Demographics Sources:\n1-Civilian Labor Force\n2-EQFXSUBPRIME\n3-People 25 and Over Education\n4-Resident Population\n\nLand Sources:\n5-All Transactions House Price Index\n6-Homeownership Rate\n7-New Private Housing')
            FRED()
        else:
            print('Please enter a numeric value for source.')    
            FRED()  
        
        while True:
            endProgram()

    #Clean Zillow data
    def ZLLW(): #done
        source = int(input('What source are you updating? Type 0 for list of sources. '))
        if source == 1:
            print('Updating Median Sale Price')
            filename = './Updates/STG_ZLLW_County_MedianSalePrice_AllHomes.txt'
            backup_fn = './Backups/STG_ZLLW_County_MedianSalePrice_AllHomes_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            df_fips = pd.read_csv('./FIPS_Codes.csv')
            df = pd.read_csv('http://files.zillowstatic.com/research/public/County/Sale_Prices_County.csv', encoding='ISO-8859-1')
            df = df.drop(columns = ['RegionID'], axis = 1)
            name_filter = df['StateName'] == "North Carolina"
            df_nc = df[name_filter]
            df_nc = df_nc.sort_values('RegionName', ascending = True)
            df_nc_join = df_nc.set_index('RegionName').join(df_fips.set_index('RegionName'))
            df_nc_join.loc[ :, 'MunicipalCodeFIPS'] = df_nc_join['MunicipalCodeFIPS'].astype(str)
            df_nc_join.loc[ :, 'MunicipalCodeFIPS'] = df_nc_join['MunicipalCodeFIPS'].str.zfill(3)
            columns = ['State','Metro','StateCodeFIPS','MunicipalCodeFIPS','SizeRank','2008-03','2008-04','2008-05','2008-06','2008-07','2008-08','2008-09','2008-10','2008-11','2008-12','2009-01','2009-02','2009-03','2009-04','2009-05','2009-06','2009-07','2009-08','2009-09','2009-10','2009-11','2009-12','2010-01','2010-02','2010-03','2010-04','2010-05','2010-06','2010-07','2010-08','2010-09','2010-10','2010-11','2010-12','2011-01','2011-02','2011-03','2011-04','2011-05','2011-06','2011-07','2011-08','2011-09','2011-10','2011-11','2011-12','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12','2016-01','2016-02','2016-03','2016-04','2016-05','2016-06','2016-07','2016-08','2016-09','2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01','2018-02','2018-03','2018-04','2018-05','2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06']
            df_nc_join = df_nc_join[columns]
            df_nc_join.to_csv(filename, sep ='\t')
            pass
        elif source == 2:
            print('Updating Median Value Per Sqft')
            filename = './Updates/STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt'
            backup_fn = './Backups/STG_ZLLW_County_MedianValuePerSqft_AllHomes_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            df = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_MedianValuePerSqft_AllHomes.csv', encoding='ISO-8859-1')
            name_filter = df['State'] == 'NC'
            df_nc = df[name_filter]
            df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].astype(str)
            df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].str.zfill(3)
            df_nc.set_index(df_nc['RegionName'], inplace=True)
            df_nc.drop('RegionName', axis=1, inplace=True)
            df_nc.to_csv(filename, sep='\t')
            pass
        elif source == 3:
            print('Updating Zhvi')
            filename = './Updates/STG_ZLLW_County_Zhvi_AllHomes.txt'
            backup_fn = './Backups/STG_ZLLW_County_Zhvi_AllHomes_BACKUP.txt'
            df = pd.read_csv(filename)
            df.to_csv(backup_fn)
            df = pd.read_csv('http://files.zillowstatic.com/research/public/County/County_Zhvi_AllHomes.csv', encoding='ISO-8859-1')
            name_filter = df['State'] == 'NC'
            df_nc = df[name_filter]
            df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].astype(str)
            df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].str.zfill(3)
            df_nc.set_index(df_nc['RegionName'], inplace=True)
            df_nc.drop('RegionName', axis=1, inplace=True)
            df_nc.to_csv(filename, sep='\t')
            pass
        elif source == 0:
            print('Zillow Sources\n1-Median Sale Price\n2-Median Value Per Sqft\n3-Zhvi')
            ZLLW()
        else:
            print('Please enter a numeric value for source.')
            ZLLW()

        while True:
            endProgram()

    #Clean BEA data
    def BEA(): #done 
        #create new file for every dictionary entry
        files = {5:'https://apps.bea.gov/regional/zip/CAINC5N.zip', 6:'https://apps.bea.gov/regional/zip/CAINC6N.zip'}
        key = int(input('What source are you updating? Type 0 for list of sources. '))
        if key == 5:
            for key, value in files.items():
                response = requests.get(value)
                zip = ZipFile(BytesIO(response.content))
                files = zip.namelist()
                with zip.open(files[34]) as csvfile:
                    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
                    df.drop(df.tail(4).index,inplace=True)
                    df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
                    df.set_index(df['GeoFIPS'], inplace = True)
                    df.drop('GeoFIPS', axis = 1, inplace = True)
                    version = int(input('Are you updating the Labor:1 or Earnings:2 version? '))
                    if version == 1:
                        print('Updating Labor version.')
                        linecodes = {10:'STG_BEA__CA5N_PersonalIncome.txt', 20:'STG_BEA_CA5N_Population.txt', 30:'STG_BEA_CA5N_Per_Capita_Personal_Income.txt', 35:'STG_BEA_CA5N_Earnings_by_Place_of_Work.txt'}
                        for key, value in linecodes.items():
                            filter1 = df['LineCode'] == key
                            df_filtered = df[filter1]
                            df_filtered.to_csv(value, sep = '\t')
                            pass
                    elif version == 2:
                        print('Updating Earnings Version.')
                        linecodes = {10:'STG_BEA_CA5N_PersonalIncome.txt', 20:'STG_BEA_CA5N_Population.txt', 30:'STG_BEA_CA5N_Per_Capita_Personal_Income.txt', 35:'STG_BEA_CA5N_Earnings_by_Place_of_Work.txt', 50:'STG_BEA_CA5N_Wages_and_Salaries.txt', 60:'STG_BEA_CA5N_Supplements_to_Wages_and_Salaries.txt', 70:'STG_BEA_CA5N_Proprietors_Income.txt', 81:'STG_BEA_CA5N_Farm_Earnings.txt', 82:'STG_BEA_CA5N_Nonfarm_Earnings.txt', 90:'STG_BEA_CA5N_Private_NonFarm_Earnings.txt', 100:'STG_BEA_CA5N_Forestry_Fishing_and_Related_Activities.txt', 200:'STG_BEA_CA5N_Mining_Quarrying_and_Oil_and_Technical_Services.txt', 300:'STG_BEA_CA5N_Utilities.txt', 400:'STG_BEA_CA5N_Construction.txt', 500:'STG_BEA_CA5N_Manufacturing.txt', 600:'STG_BEA_CA5N_Wholesale_Trade.txt', 700:'STG_BEA_CA5N_Retail_Trade.txt', 800:'STG_BEA_CA5N_Transporatation_and_Warehousing.txt', 900:'STG_BEA_CA5N_Information.txt', 1000:'STG_BEA_CA5N_Finance_and_Insurance.txt', 1100:'STG_BEA_CA5N_Real_Estate_and_Rental_and_Leasing.txt', 1200:'STG_BEA_CA5N_Professional_Scientific_and_Technical_Services.txt', 1300:'STG_BEA_CA5N_Management_of_Companies_and_Enterprises.txt', 1400:'STG_BEA_CA5N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', 1500:'STG_BEA_CA5N_Educational_Services.txt', 1600:'STG_BEA_CA5N_Health_Care_and_Social_Assistance.txt', 1700:'STG_BEA_CA5N_Arts_Entertainment_and_Recreation.txt', 1800:'STG_BEA_CA5N_Accommodation_and_Food_Services.txt', 1900:'STG_BEA_CA5N_Other_Services.txt', 2000:'STG_BEA_CA5N_Government_and_Government_Enterprises.txt', 2001:'STG_BEA_CA5N_Federal_Civilian.txt', 2002:'STG_BEA_CA5N_Military.txt', 2010:'STG_BEA_CA5N_State_and_Local.txt', 2011:'STG_BEA_CA5N_State_Government.txt', 2012:'STG_BEA_CA5N_Local_Government.txt'}
                        for key, value in linecodes.items():
                            filter1 = df['LineCode'] == key
                            df_filtered = df[filter1]
                            df_filtered.to_csv(value, sep = '\t')
                            pass
        elif key == 6:    
            for key, value in files.items():
                response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
                zip = ZipFile(BytesIO(response.content))
                files = zip.namelist()
                with zip.open(files[34]) as csvfile:
                    df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
                    df.drop(df.tail(4).index,inplace=True)
                    df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
                    df.set_index(df['GeoFIPS'], inplace = True)
                    df.drop('GeoFIPS', axis = 1, inplace = True)
                    linecodes = {1:'STG_BEA_CA6N_Compensation_of_Employees.txt', 5:'STG_BEA_CA6N_Wages_and_Salaries.txt', 6:'STG_BEA_CA6N_Supplements_to_Wages_and_Salaries.txt', 7:'STG_BEA_CA6N_Employer_Contributions_for_Employee_Pension_and_Insurance_Funds.txt', 8:'STG_BEA_CA6N_Employer_Contributions_for_Government_Social_Insurance.txt', 9:'STG_BEA_CA6N_Average_Compensation_Per_Job.txt', 81:'STG_BEA_CA6N_Farm_Compensation.txt', 82:'STG_BEA_CA6N_NonFarm_Compensation.txt', 90:'STG_BEA_CA6N_Private_Nonfarm_Compensation.txt', 100:'STG_BEA_CA6N_Average_Compensation_Per_Job.txt', 200:'STG_BEA_CA6N_Mining_Quarrying_and_Oil_and_Gas_Extraction.txt', 300:'STG_BEA_CA6N_Utilities.txt', 400:'STG_BEA_CA6N_Construction.txt', 500:'STG_BEA_CA6N_Manufacturing.txt', 600:'STG_BEA_CA6N_Wholesale_Trade.txt', 700:'STG_BEA_CA6N_Retail_Trade.txt', 800:'STG_BEA_CA6N_Transportation_and_Warehousing.txt', 900:'STG_BEA_CA6N_Information.txt', 1000:'STG_BEA_CA6N_Finance_and_Insurance.txt', 1100:'STG_BEA_CA6N_Real_Estate_and_Rental_and_Leasing.txt', 1200:'STG_BEA_CA6N_Professional_Scientific_and_Technical_Services.txt', 1300:'STG_BEA_CA6N_Management_of_Companies_and_Enterprises.txt', 1400:'STG_BEA_CA6N_Administrative_and_Support_and_Waste_Management_and_Remediation_Services.txt', 1500:'STG_BEA_CA6N_Educational_Services.txt', 1600:'STG_BEA_CA6N_Health_Care_and_Social_Assistance.txt', 1700:'STG_BEA_CA6N_Arts_Entertainment_and_Recreation.txt', 1800:'STG_BEA_CA6N_Accommodation_and_Food_Services.txt', 1900:'STG_BEA_CA6N_Other_Services.txt', 2000:'STG_BEA_CA6N_Government_and_Government_Enterprises.txt'}
                    for key, value in linecodes.items():
                        filter1 = df['LineCode'] == key
                        df_filtered = df[filter1]
                        df_filtered.to_csv(value, sep='\t')
                        pass
        elif key == 0:
            print('5-CAINC5N\n6-CAINC6N')
            BEA()
        else:
            print('Please enter a numeric value for source.')
            BEA()

        while True: 
            endProgram()
    
    #Clean NC Tax data
    def NCDOR(): #done
        whatfile = int(input('Which file would you like to update: 0001:1 or 0002:2? '))
        if whatfile == 1:
            month = input('Please enter link to new month tax data: ')
            df = pd.read_excel(month, skiprows=9)
            df = df.drop(df.index[0])
            df = df[:-8]
            df = df.loc[:,~df.columns.str.contains('Unnamed')]
            df2 = pd.DataFrame(df, columns = ['County.1', 'Collections*.1'])
            df = df.drop(columns=['County.1', 'Collections*.1', 'and Purchases*', 'and Purchases*.1'])
            df = df.rename(columns = {'County.1':'County', 'Collections*':'Collections'})
            df2 = df2.rename(columns = {'County.1':'County', 'Collections*.1':'Collections'})
            df_append = df.append(df2, ignore_index=True)
            df_append = df_append.dropna(how='all')
            df_append = df_append.fillna('0')
            df_append['Collections'] = df_append['Collections'].astype(float)
            df_append = df_append[:-5]
            df_append.to_csv('./Updates/0STG_BEA_MSALESUSETAX_0001.txt', sep='\t')
            pass
        elif whatfile == 2:
            month = input('Please enter link to new month tax data: ')
            df = pd.read_excel(month, skiprows =  9)
            df = df.drop(df.index[0])
            df = df[:-8]
            df = df.loc[:,~df.columns.str.contains('Unnamed')]
            df2 = pd.DataFrame(df, columns = ['County.1', 'and Purchases*.1'])
            df = df.drop(columns=['County.1', 'Collections*', 'Collections*.1', 'and Purchases*.1'])
            df = df.rename(columns = {'County.1':'County', 'and Purchases*':'Sales'})
            df2 = df2.rename(columns = {'County.1':'County', 'and Purchases*.1':'Sales'})
            df_append = df.append(df2, ignore_index=True)
            df_append = df_append.dropna(how='all')
            df_append = df_append.fillna('0')
            df_append['Sales'] = df_append['Sales'].astype(float)
            df_append = df_append[:-5]
            df_append.to_csv('./Updates/STG_BEA_MSALESUSETAX_0002.txt', sep='\t')
            pass
        else:
            print('this broke')
            pass
        
        while True:
            endProgram()

    #Updating Demographics section
    def demographics_update(): #done
        number_ofs = int(input('----------------------------------------------\nWelcome to Demographics!\nHow many files are you updating? '))
        for i in range(number_ofs):
            source = int(input('Are you updating GeoFRED:1 or CENSUS:2 data? '))
            if source == 1:
                FRED()
            elif source == 2:
                CNSUS()
            else:
                print('Please enter 1 for GeoFred or 2 for Census.')
                demographics_update()
            
        while True:
            endProgram()

    #Updating Earnings section
    def earnings_update(): #done 
        number_ofs = int(input('----------------------------------------------\nWelcome to Earnings!\nHow many files are you updating? '))
        for i in range(number_ofs):
            source = int(input('Are you updating BEA:1 or NC State Tax:2 data? '))
            if source == 1:
                BEA()
            elif source == 2:
                NCDOR()
        else:
            print('Please enter 1 for BEA or 2 for NC State Tax.')
            earnings_update()

        while True:
            endProgram()

    #Updating Health section
    def health_update(): # needs data, building
        print('----------------------------------------------\nWelcome to Health!\nneeds building')

    #Updating Labor section
    def labor_update(): # done
        number_ofs = int(input('----------------------------------------------\nWelcome to Labor!\nHow many files are you updating? '))
        for i in range(number_ofs):
            source = int(input('Are you updating GeoFRED:1 or BEA:2 data? '))
            if source == 1:
                FRED()
            elif source == 2:
                BEA()
        else:
            print('Please enter 1 for GeoFred or 2 for BEA.')
            labor_update()

        while True:
            endProgram()

    #Updating Land section
    def land_update(): #done
        number_ofs = int(input('----------------------------------------------\nWelcome to Land!\nHow many files are you updating? '))
        for i in range(number_ofs):
            source = int(input('Are you updating ZLLW:1 or GeoFRED:2? '))
            if source == 1:
                ZLLW()
            elif source == 2:
                FRED()
        else:
            print('Please enter 1 for ZLLW or 2 for GeoFRED.')
            pass

        while True:
            endProgram()

    #Updating Natural Products section
    def natproducts_update(): #done
        print('----------------------------------------------\nWelcome to Natural Products!')
        df = pd.read_excel('./Data/TableauData_NC_NaturalProducts_Section.xlsx')
        column_list = df.columns.values
        for i in column_list:
            df.loc[df[i].isnull(),i]=''
        df.to_csv('./Updates/STG_Natural_Products.txt', sep='\t')

        while True:
            endProgram()

    while True: #done
        runProgram()
        demographics_update()
        earnings_update()
        health_update()
        labor_update()
        land_update()
        natproducts_update()
        endProgram()

except KeyboardInterrupt:
    endProgram()