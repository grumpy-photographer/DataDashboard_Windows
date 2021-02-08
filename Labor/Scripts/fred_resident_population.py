# Imports
import pandas as pd

# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1970-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    skiprows=1,
)

df["Region Code"] = df["Region Code"].astype(str)
df["Region Code"] = df["Region Code"].str.zfill(5)

# create measure column
df["Measure Name"] = "Resident Population"

# Drop Series ID column
df.drop("Series ID", axis=1, inplace=True)

# Pivot data
df = df.melt(id_vars=["Region Code", "Region Name", "Measure Name"],
             value_name="Estimated Value", var_name="Date")
df.set_index("Region Code", inplace=True)

# Set Date column to date dtype
df["Date"] = pd.to_datetime(df["Date"])
df["Date"] = df["Date"].dt.date

column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(), i] = 0

# Save file to tab delimited txt for upload to SSMS
df.to_csv(
    "./Updates/FRED_Resident_Population_by_County_Thousands_of_Persons.txt",
    sep="\t",
)
