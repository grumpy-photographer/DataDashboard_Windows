# Imports
import pandas as pd

# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Persons%2C+no_period_desc&aggregationFrequency=Monthly&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1262&hideLegend=false",
    skiprows=1,
)

# Set Series ID as index
df.set_index(df["Region Code"], inplace=True)

# Drop Series ID column
df.drop("Region Code", axis=1, inplace=True)
df.drop("Series ID", axis=1, inplace=True)

column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(), i] = 0

# Save file to tab delimited txt for upload to SSMS
df.to_csv("./Updates/FRED_Civilian_Labor_Force_by_County_Persons.txt", sep="\t")
