import pandas as pd

df_race = pd.read_excel('../Data/NC_COVID-19/4.24.2020/ByRace&Ethnicity.xlsx')
print(df_race.head())

df_cls = pd.read_excel('../Data/NC_COVID-19/4.24.2020/ConfirmedCases&Deaths in CLS.xlsx')
print('\n', df_cls.head())

df_outbreaks = pd.read_excel('../Data/NC_COVID-19/4.24.2020/Ongoing Outbreaks in CLS.xlsx')
print('\n', df_outbreaks.head())