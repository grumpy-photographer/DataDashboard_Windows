# Imports
import pandas as pd

# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1224&attributes=Not+Seasonally+Adjusted%2C+Monthly%2C+Percent%2C+no_period_desc&aggregationFrequency=Monthly&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1976-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    skiprows=1,
)

df["Region Code"] = df["Region Code"].astype(str)
df["Region Code"] = df["Region Code"].str.zfill(5)

df["Measure Name"] = "Unemployment Rate"

# Drop Series ID column
df.drop("Series ID", axis=1, inplace=True)

# Pivot data
df = df.melt(id_vars=["Region Code", "Region Name", "Measure Name"],
             value_name="Estimated Value", var_name="Date")
df.set_index("Region Code", inplace=True)

column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(), i] = 0

# Save file to tab delimited txt for upload to SSMS
df.to_csv("./Updates/FRED_Unemployment_Rate_by_County_Percent.txt", sep="\t")
