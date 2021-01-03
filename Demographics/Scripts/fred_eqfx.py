# Imports
import pandas as pd

# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=39.98&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147149&attributes=Not+Seasonally+Adjusted%2C+Quarterly%2C+Percent%2C+no_period_desc&aggregationFrequency=Quarterly&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1999-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    skiprows=1,
)

# Set Series ID as index
df["Region Code"] = df["Region Code"].astype(str)
df["Region Code"] = df["Region Code"].str.zfill(5)
df.set_index("Region Code", inplace=True)

# create measure column
df["Measure_Name"] = "Equifax Subprime Credit Population"

# Drop Series ID column
df.drop("Series ID", axis=1, inplace=True)

column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(), i] = 0

# Save file to tab delimited txt for upload to SSMS
df.to_csv("./Updates/FRED_EQFXSUBPRIME.txt", sep="\t", encoding="UTF-8")