# Imports
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import requests

# Load BEA CAINC5N_NC data
response = requests.get("https://apps.bea.gov/regional/zip/CAINC5N.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[34]) as csvfile:
    df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")

# Remove unused fields
df.drop(df.tail(4).index, inplace=True)

# Remove quotes from GeoFIPS
df["GeoFIPS"] = df["GeoFIPS"].str.replace('"', "")
df = df.rename(columns={"GeoFIPS": "Region Code",
                        "Description": "Measure Name", "GeoName": "Region Name"})

# Set Region Code as Index
df.set_index(df["Region Code"], inplace=True)

# Drop Region Code column
df.drop(columns = ["Region Code", "Region", "TableName"], axis=1, inplace=True)

# # Create Per Capita Personal Income
# Create new dataframe for Per capita personal income
filter1 = df["LineCode"] == 30
df_per_capita = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_per_capita.to_csv(
    "./Updates/BEA_NC_CA5N_Per_Capita_Personal_Income.txt", sep="\t")


# # Create Earnings by Place of Work
# Create a new dataframe for Earnings by place of work
filter1 = df["LineCode"] == 35
df_earnings = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_earnings.to_csv(
    "./Updates/BEA_NC_CA5N_Earnings_by_Place_of_Work.txt", sep="\t")


# # Create Population
# Create a new dataframe for Population
filter1 = df["LineCode"] == 20
df_population = df[filter1]

# Clean Description column
df_population.loc[:,
                  "Description"] = df_population["Description"].str.strip("2/")

# Save as tab-delimited txt file for export to SSMS
df_population.to_csv("./Updates/BEA_NC_CA5N_Population.txt", sep="\t")


# # Create Personal Income
# Create new dataframe for Personal Income
filter1 = df["LineCode"] == 10
df_income = df[filter1]

# Save as tab-delimited txt file for export to SSMS
df_income.to_csv("./Updates/BEA_NC_CA5N_Personal_Income.txt", sep="\t")
