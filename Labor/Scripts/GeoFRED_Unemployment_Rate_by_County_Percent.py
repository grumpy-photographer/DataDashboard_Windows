# Imports
import pandas as pd

# Create backups
df_backup = pd.read_csv(
    "./Updates/FRED_Unemployment_Rate_by_County_Percent.txt")
df_backup.to_csv(
    "./Backups/FRED_Unemployment_Rate_by_County_Percent_BACKUP.txt")

# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1224&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Percent%2C+no_period_desc&aggregationFrequency=Monthly&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1976-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    skiprows=1,
)

# Set Index to Series ID
df.set_index(df["Series ID"], inplace=True)

# Drop Series ID column
df.drop("Series ID", axis=1, inplace=True)

# Save file to tab delimited txt for upload to SSMS
df.to_csv("./Updates/FRED_Unemployment_Rate_by_County_Percent.txt", sep="\t")
