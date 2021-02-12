# Imports
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import requests

response = requests.get("https://apps.bea.gov/regional/zip/CAINC5N.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[0]) as csvfile:
    df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",", low_memory=False)

df.drop(df.tail(4).index, inplace=True)

df["LineCode"] = df["LineCode"].astype(int)
df["GeoFIPS"] = df["GeoFIPS"].str.replace('"', "")
df["Description"] = df["Description"].str.strip()
df.drop(
    ["TableName", "Region", "IndustryClassification", "Unit"],
    axis=1,
    inplace=True,
)

linecodes = [
    10,
    20,
    30,
    35,
    50,
    60,
    70,
    81,
    82,
    90,
    100,
    200,
    300,
    400,
    500,
    600,
    700,
    800,
    900,
    1000,
    11000,
    1200,
    1300,
    1400,
    1500,
    1600,
    1700,
    1800,
    1900,
    2000,
    2001,
    2002,
    2010,
    2011,
    2012,
]
df = df[df["LineCode"].isin(linecodes)]

column_list = df.columns.values
for i in column_list:
    df.loc[df[i].isnull(), i] = 0

df = df.rename(
    columns={
        "GeoFIPS": "Region Code",
        "GeoName": "Region Name",
        "Description": "Measure Name",
    }
)
df = df.drop(columns="LineCode", axis=1)

df_melt = df.melt(
    id_vars=["Region Code", "Region Name", "Measure Name"],
    var_name="Date",
    value_name="Estimated Value",
)

df_melt["Date"] = pd.to_datetime(df_melt["Date"])
df_melt["Date"] = df_melt["Date"].dt.date

df_melt["Measure Name"] = df_melt["Measure Name"].str.replace("2/", "")
df_melt["Measure Name"] = df_melt["Measure Name"].str.replace("7/", "")
df_melt["Estimated Value"] = df_melt["Estimated Value"].str.replace("(D)", "")
df_melt["Measure Name"] = df_melt["Measure Name"].str.strip()
df_melt = df_melt.set_index("Region Code")
df_melt = df_melt[~(df_melt["Region Name"] == "United States")]

df_melt.to_csv("./Updates/BEA_CA5N_National.txt", sep="\t")
