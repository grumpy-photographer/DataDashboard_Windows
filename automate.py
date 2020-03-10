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

def runProgram():
    folder = int(input('Which folder would you like to clean data for?\nType 0 for list of options. '))
    if folder == 0:
        print('To clean data for the Demographics, type 1.\nTo clean data for Earnings, type 2.\nTo clean data for Health, type 3.\nTo clean data for Labor, type 4.\nTo clean data for Land, type 5.\nTo exit, type 999.')
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
    elif folder == 999:
        print('Have a nice day!')
        exit()
    else:
        print('Something went wrong.  Please try again.')

    while True:
        end = input('Would you like to run the program again? ')
        if end == 'Yes' or end == 'yes':
            runProgram()
        elif end == 'No' or end == 'no':
            print('Thanks!')
            exit()
        else:
            print('Please enter Yes or No.')

def endProgram():
    answer = int(input('Would you like to update the folder again? 1 = Yes or 2 = No.\n'))
    if answer == 1:
            demographics_update()
            earnings_update()
            health_update()
            labor_update()
            land_update()           
    elif answer == 2:
            answer2 = int(input('Return to main menu? 1 = Yes or 2 = No.\n'))
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

def createBackups():
    df = pd.read_csv('')
    df.to_csv('_BACKUP')

def fredClean(): #broken
    df = pd.read_excel(file, skiprows=1)
    region_abbr_filter = df['Region Name'].str.contains(', NC')
    df_nc = df[region_abbr_filter]
    df_nc.set_index(df_nc['Series ID'], inplace = True)
    df_nc.drop('Series ID', axis = 1, inplace = True)
    filename = input('What would you like to save the file as?\n')
    df_nc.to_csv(filename, sep = '\t')        

def BEA():
    whatfile = int(input('What file are you updating: CAINC5N:5 or CAINC6N:6? '))
    if whatfile == 5:
        try:
            response = requests.get('https://apps.bea.gov/regional/zip/CAINC5N.zip')
            zip = ZipFile(BytesIO(response.content))
            files = zip.namelist()
            with zip.open(files[34]) as csvfile:
                df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
                df.drop(df.tail(4).index,inplace=True)
                df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
                df.set_index(df['GeoFIPS'], inplace = True)
                df.drop('GeoFIPS', axis = 1, inplace = True)
                version = int(input('Are you updating the Labor:1 or Earnings version:2? '))
                if version == 1: 
                    print('Updating Labor version.')
                    linecodes = {10:'Personal Income', 20:'Population', 30:'Per Capita Personal Income', 35:'Earnings by Place of Work', }
                    for code in linecodes:
                        filter1 = df['LineCode'] == code
                        df_filtered = df[filter1]
                        df_filtered.to_csv('Labor_CAINC5N.txt', sep = '\t')
                        pass
                elif version ==2:
                    print('Updating Earnings version.')
                    linecodes = {10:'Personal Income', 20:'Population', 30:'Per Capita Personal Income', 35:'Earnings by Place of Work', 50:'Wages and Salaries', 60:'Supplements to Wages and Salaries', 70:'Proprietors Income', 81:'Farm Earnings', 82:'NonFarm Earnings', 90:'Private NonFarm Earnings', 100:'Forestry, Fishing, etc', 200:'Mining, Quarrying, etc', 300:'Utilities', 400:'Construction', 500:'Manufacturing', 600:'Wholesale Trade', 700:'Retail Trade', 800:'Transporatation and Warehousing', 900:'Information', 1000:'Finance and Insurance', 1100:'Real Estate and Rental and Leasing', 1200:'Pro Scientific/Tech', 1300:'Mgt Companies and Ent', 1400:'Admin and Support and Waste Mgt', 1500:'Edu Services', 1600:'Health Care and Social Assistance', 1700:'Arts, Entertainment,etc', 1800:'Accommodation and Food Services', 1900:'Other Services', 2000:'Govt and Govt Ent', 2001:'Federal, Civilian Govt', 2002:'Military Govt', 2010:'State and Local Govt', 2011:'State Govt', 2012:'Local Govt'}
                    for code in linecodes:
                        filter1 = df['LineCode'] == code
                        df_filtered = df[filter1]
                        df_filtered.to_csv('Earnings_CAINC5N.txt', sep = '\t')
                        pass
                else:
                    print('this broke')
                    pass
        except:
            print('That did not work.')
            endProgram()
    elif whatfile == 6:
        response = requests.get('https://apps.bea.gov/regional/zip/CAINC6N.zip')
        zip = ZipFile(BytesIO(response.content))
        files = zip.namelist()
        with zip.open(files[34]) as csvfile:
            df = pd.read_csv(csvfile, encoding='ISO-8859-1', sep=",")
            df.drop(df.tail(4).index,inplace=True)
            df['GeoFIPS'] = df['GeoFIPS'].replace({"":''})
            df.set_index(df['GeoFIPS'], inplace = True)
            df.drop('GeoFIPS', axis = 1, inplace = True)
            linecodes = {1:'Compensation of Employees', 5:'Wage and Salaries', 6:'Supplements to Wages and Salaries', 7:'Employer Contributions for Employee Pension and Insurance Funds', 8:'Employer Contributions for Govt Social Insurance', 9:'Average Comp per Job', 81:'Farm Comp', 82:'NonFarm Comp', 90:'Priv NonFarm Comp', 100:'Forestry,Fishing, etc', 200:'Mining, Quarrying, etc', 300:'Utilities', 400:'Construction', 500:'Manufacturing', 600:'Wholesale Trade', 700:'Retail Trade', 800:'Transportation and Warehousing', 900:'Information', 1000:'Finance and Insurance', 1100:'Real Estate and Rental and Leasing', 1200:'Pro Scientific/Tech', 1300:'Mgt of Co and Ent', 1400:'Admin Support', 1500:'Edu Services', 1600:'Health Care and Social Assistance', 1700:'Arts,Ent,Rec', 1800:'Acco/Food Services', 1900:'Other Services', 2000:'Govt/Govt Enter'}
            for code in linecodes:
                filter1 = df['LineCode'] == code
                df_filtered = df[filter1]
                df_filtered.to_csv('Labor_CAINC6N.txt', sep='\t')
                pass
    while True: 
        endProgram()
 
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

def demographics_update(): #need fred
    number_ofs = int(input('How many files are you updating? '))
    for i in range(number_ofs):
        source = input('Are you updating GeoFRED:1 or CENSUS:2 data? ')
        if source == 1:
            fredClean()
        elif source == 2:
            censusClean() # does not exist
        else:
            print('Please enter 1 for GeoFred or 2 for Census.')
            demographics_update()
        
    while True:
        endProgram()

def earnings_update(): #needs fred
    number_ofs = int(input('How many files are you updating? '))
    for i in range(number_ofs):
        source = int(input('Are you updating GeoFRED:1 or NC State Tax:2 data? '))
        if source == 1:
            fredClean()
        elif source == 2:
            NCDOR()
    else:
        print('Please enter 1 for GeoFred or 2 for NC State Tax.')
        earnings_update()

    while True:
        endProgram()

def health_update(): # needs everything
    number_ofs = int(input('How many files are you updating? '))
    for i in range(number_ofs):
        try:
            file = str(input('Enter web address for file: '))
        except FileNotFoundError:
            print('Cannot find data from web address.')
            pass
    else:
        pass

    while True:
        endProgram()

def labor_update(): # done
    number_ofs = int(input('How many files are you updating? '))
    for i in range(number_ofs):
        source = int(input('Are you updating GeoFRED:1 or BEA:2 data? '))
        if source == 1:
            fredClean()
        elif source == 2:
            BEA()
    else:
        print('Please enter 1 for GeoFred or 2 for BEA.')
        labor_update()

    while True:
        endProgram()

#def land_update(): #needs fred
    number_ofs = int(input('How many files are you updating?\n'))
    for i in range(number_ofs):
        try:
            file = str(input('Enter web address for file: '))
            if 'MedianValuePerSqft' in file:
                print('Cleaning Median Value Per Sqft data.')
                df = pd.read_csv(file, encoding='ISO-8859-1')
                state_abbr_filter = df['State'] == 'NC'
                df_nc = df[state_abbr_filter]
                df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].astype(str)
                df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].str.zfill(3)
                df_nc.set_index(df_nc['RegionName'], inplace=True)
                df_nc.drop('RegionName', axis=1, inplace=True)
                df_nc.to_csv('STG_ZLLW_County_MedianValuePerSqft_AllHomes.txt', sep='\t')
            elif 'Zhvi' in file:
                print('Cleaning ZHVI data.')
                df = pd.read_csv(file, encoding='ISO-8859-1')
                state_abbr_filter = df['State'] == 'NC'
                df_nc = df[state_abbr_filter]
                df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].astype(str)
                df_nc.loc[:, 'MunicipalCodeFIPS'] = df_nc['MunicipalCodeFIPS'].str.zfill(3)
                df_nc.set_index(df_nc['RegionName'], inplace=True)
                df_nc.drop('RegionName', axis=1, inplace=True)
                df_nc.to_csv('STG_ZLLW_County_Zhvi_AllHomes.txt', sep='\t')
            elif 'Sales' in file:
                print('Cleaning Median Sale Price data.')
                df_fips = pd.read_csv('./FIPS_Codes.csv')
                df = pd.read_csv(file, encoding='ISO-8859-1')
                df = df.drop(columns = ['RegionID'], axis = 1)
                state_name_filter = df['StateName'] == "North Carolina"
                df_nc = df[state_name_filter]
                df_nc = df_nc.sort_values('RegionName', ascending = True)
                df_nc_join = df_nc.set_index('RegionName').join(df_fips.set_index('RegionName'))
                df_nc_join.loc[ :, 'MunicipalCodeFIPS'] = df_nc_join['MunicipalCodeFIPS'].astype(str)
                df_nc_join.loc[ :, 'MunicipalCodeFIPS'] = df_nc_join['MunicipalCodeFIPS'].str.zfill(3)
                columns = ['State','Metro','StateCodeFIPS','MunicipalCodeFIPS','SizeRank','2008-03','2008-04','2008-05','2008-06','2008-07','2008-08','2008-09','2008-10','2008-11','2008-12','2009-01','2009-02','2009-03','2009-04','2009-05','2009-06','2009-07','2009-08','2009-09','2009-10','2009-11','2009-12','2010-01','2010-02','2010-03','2010-04','2010-05','2010-06','2010-07','2010-08','2010-09','2010-10','2010-11','2010-12','2011-01','2011-02','2011-03','2011-04','2011-05','2011-06','2011-07','2011-08','2011-09','2011-10','2011-11','2011-12','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12','2016-01','2016-02','2016-03','2016-04','2016-05','2016-06','2016-07','2016-08','2016-09','2016-10','2016-11','2016-12','2017-01','2017-02','2017-03','2017-04','2017-05','2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01','2018-02','2018-03','2018-04','2018-05','2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01']
                df_nc_join = df_nc_join[columns]
                df_nc_join.to_csv('STG_ZLLW_County_MedianSalePrice_AllHomes.txt', sep ='\t')
            else:
                fredClean()
        except FileNotFoundError:
            print('Cannot find data from web address.')
            pass
    else:
        pass

    while True:
        endProgram()

while True:
    runProgram()
    demographics_update()
    earnings_update()
    health_update()
    labor_update()
    land_update()
    endProgram()