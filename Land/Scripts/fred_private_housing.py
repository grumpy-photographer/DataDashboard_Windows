# Imports
import pandas as pd

# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=155206&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Units%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1253&hideLegend=false",
    skiprows=1,
)

# Set Index to Series ID
df.set_index(df["Series ID"], inplace=True)

# Drop Series ID column
df.drop("Series ID", axis=1, inplace=True)


# Save file to tab delimited txt for upload to SSMS
df.to_csv("./Updates/FRED_New_Private_Housing_Structures_Authorized_by_Building_Permits_by_County.txt", sep="\t")
