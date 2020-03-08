import pandas as pd
import numpy as np

def update_file():
    number_of_files = int(input('How many files are you updating? '))
    for address in range(number_of_files):
        file = str(input('Enter web address for file: '))
        df = pd.read_csv(file, encoding = 'ISO-8859-1')
        print(df.head())
    else:
        pass

    while True:
        answer = input('Would you like to run the program again? ')
        if answer == 'yes':
            break
        else:
            print('Thanks!')
            exit()

while True:
    update_file()