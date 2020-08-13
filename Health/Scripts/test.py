import pandas as pd

df_race = pd.read_excel('../Data/NC_COVID-19/4.24.2020/ByRace&Ethnicity.xlsx')
print(df_race.head())

import glob

l = [pd.read_excel(filename) for filename in glob.glob("../Data/NC_COVID-19/*/*.excel")]
df = pd.concat(1, axis=0)
print(df)