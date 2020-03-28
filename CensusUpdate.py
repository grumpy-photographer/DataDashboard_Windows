import pandas as pd
import os

def CNSUS(): #working
    os.chdir('./Demograhics')
    source = int(input('What source are you updating? '))
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
    elif source == 0:
        print('\nCensus Sources:\n1-PEPAGESEX USA\n2-PEPAGESEX NC\n3-PEPSR6H NC\n4-PEPAGESEX County\n5-PEPSR6H County\n\n999-Exit\n')
        CNSUS()
    elif source == 999:
        exit()
    else:
        print('Please enter a number from the menu.')
        CNSUS()
    while True:
        CNSUS()