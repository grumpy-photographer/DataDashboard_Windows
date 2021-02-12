from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import numpy as np
import requests

response = requests.get("https://apps.bea.gov/regional/zip/CAGDP2.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()

df_master = pd.DataFrame([])

for file in files:
    if ".csv" in file and "ALL" not in file:
        with zip_file.open(file) as csvfile:

            df = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")

            df = df.rename(
                columns={
                    "GeoFIPS": "Region Code",
                    "Description": "Measure_Name",
                    "GeoName": "Region Name",
                }
            )

            df["LineCode"] = df["LineCode"].astype("str")

            # Remove quotes from GeoFIPS
            df["Region Code"] = df["Region Code"].str.replace('"', "")

            # Drop rows at the end of table
            df.drop(df.tail(4).index, inplace=True)

            # Remove rows that are not needed
            df = df.drop(
                columns=[
                    "Region",
                    "TableName",
                    "LineCode",
                    "IndustryClassification",
                    "Unit",
                ]
            )

            # Strip whitespace from object type columns
            df_obj = df.select_dtypes(["object"])
            df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

            measures = [
                "Agriculture, forestry, fishing and hunting",
                "Mining, quarrying, and oil and gas extraction",
                "Utilities",
                "Construction",
                "Manufacturing",
                "Wholesale trade",
                "Retail trade",
                "Transportation and warehousing",
                "Information",
                "Finance and insurance",
                "Real estate and rental and leasing",
                "Professional and business services",
                "Management of companies and enterprises",
                "Administrative and support and waste management and remediation services",
                "Educational services",
                "Health care and social assistance",
                "Arts, entertainment, and recreation",
                "Accommodation and food services",
                "Other services (except government and government enterprises)",
                "Government and government enterprises",
            ]

            df = df.query("Measure_Name in @measures")
            df = df.rename(columns={"Measure_Name": "Measure Name"})

            df = df.melt(
                id_vars=["Region Code", "Region Name", "Measure Name"],
                var_name="Date",
                value_name="Estimated Value",
            )

            df["Estimated Value"] = df["Estimated Value"].replace("(D)", np.nan)

            df.dropna(inplace=True)

            columns = [
                "Region Code",
                "Region Name",
                "Measure Name",
                "Date",
                "Estimated Value",
            ]

            df = df[columns]

            df["Region Code"] = df["Region Code"].str.lstrip()

            df.set_index("Region Code", inplace=True)

            df["Date"] = pd.to_datetime(df["Date"])
            df["Date"] = df["Date"].dt.date

            df_master = df_master.append(df)

df_master.to_csv("./Updates/GPD.txt", sep="\t")
