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

suffix_to_strip=["2/", "3/", "4/", "5/", "6/", "7/", "8"]
for suffix in suffix_to_strip:
    df["Measure Name"] = df["Measure Name"].str.strip(suffix)

rows_to_remove = ["Less:", "Plus:", "Equals:"]
for prefix in rows_to_remove:
    df = df[~df["Measure Name"].str.contains(prefix)]

df["Region Code"] = df["Region Code"].str.strip()
df["Measure Name"] = df["Measure Name"].str.strip()

# Drop Region Code column
df.drop(columns = ["Region", "TableName", "IndustryClassification", "Unit", "LineCode"], axis=1, inplace=True)

# Melt df
df = df.melt(id_vars=["Region Code", "Region Name", "Measure Name"],
             value_name = "Estimated Value", var_name="Date")

df["Estimated Value"] = df["Estimated Value"].replace("(D)", "")
df["Estimated Value"] = df["Estimated Value"].replace("()", "")
             
df.set_index("Region Code", inplace=True)

df.to_csv("./Updates/CAINC5N_NC.txt", sep="\t")