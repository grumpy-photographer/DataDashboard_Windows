import pandas as pd
import numpy as np
import os

def start():
    folder = int(input('Which folder would you like to clean data for?\nType 0 for list of options. '))
    if folder == 0:
        print('To clean data for the Demographics, type 1.\nTo clean data for Earnings, type 2.\nTo clean data for Health, type 3.\nTo clean data for Labor, type 4.\nTo clean data for Land, type 5.')
        start()
    elif folder == 1:
        print('Taking you to Demographics...')
        os.chdir('./Demographics')
    elif folder == 2:
        print('Taking you to Earnings...')
        os.chdir('./Earnings')        
    elif folder == 3:
        print('Taking you to Health...')
        os.chdir('./Health')
    elif folder == 4:
        print('Taking you to Labor...')
        os.chdir('./Labor')
    elif folder == 5:
        print('Taking you to Land...')
        os.chdir('./Land')    
    else:
        print('Something went wrong.  Please try again.')

    while True:
        end = input('Would you like to clean another folder? ')
        if end == 'Yes' or end == 'yes':
            start()
        elif end == 'No' or end == 'no':
            exit()
        else:
            print('Please enter Yes or No.')

while True:
    start()