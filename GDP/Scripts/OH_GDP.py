#!/usr/bin/env python
# coding: utf-8

# In[51]:


# Imports
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import numpy as np
import requests


# ## Backups

# In[52]:


backup_df = pd.read_csv("./Updates/OH_GDP_Data.txt", sep="\t")
backup_df.to_csv("./Backups/OH_GDP_Data_BACKUP.txt", sep="\t")


# ## Process data

# In[53]:


response = requests.get("https://apps.bea.gov/regional/zip/CAGDP2.zip")
zip_file = ZipFile(BytesIO(response.content))
files = zip_file.namelist()
with zip_file.open(files[42]) as csvfile:
    CAGDP2 = pd.read_csv(csvfile, encoding="ISO-8859-1", sep=",")

# Rename columns
CAGDP2 = CAGDP2.rename(
    columns={
        "GeoFIPS": "GeoArea_FIPS",
        "Description": "Economic_Measure_Name",
        "GeoName": "GeoArea_Name",
    }
)

CAGDP2["LineCode"] = CAGDP2["LineCode"].astype("str")

# Remove quotes from GeoFIPS
CAGDP2["GeoArea_FIPS"] = CAGDP2["GeoArea_FIPS"].str.replace('"', "")

# Change ', OH' in County values to 'County'
CAGDP2["GeoArea_Name"] = CAGDP2["GeoArea_Name"].str.replace(", OH", "County")

# Drop rows at the end of table
CAGDP2.drop(CAGDP2.tail(4).index, inplace=True)

# Remove rows that are not needed
CAGDP2 = CAGDP2.drop(
    columns=["Region", "TableName", "LineCode",
             "IndustryClassification", "Unit"]
)

# Strip whitespace from object type columns
CAGDP2_obj = CAGDP2.select_dtypes(["object"])
CAGDP2[CAGDP2_obj.columns] = CAGDP2_obj.apply(lambda x: x.str.strip())

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

CAGDP2 = CAGDP2.query("Economic_Measure_Name in @measures")

CAGDP2 = CAGDP2.dropna()

# Melt data
CAGDP2 = CAGDP2.melt(
    id_vars=["GeoArea_FIPS",
             "GeoArea_Name", "Economic_Measure_Name"],
    var_name="Data_Period_Business_Key",
    value_name="GDP",
)

# Replace '(D)' with Nan
CAGDP2["GDP"] = CAGDP2["GDP"].replace("(D)", np.NaN)

# In[56]:


columns = [
    "GeoArea_FIPS",
    "GeoArea_Name",
    "Economic_Measure_Name",
    "Data_Period_Business_Key",
    "GDP",
]

CAGDP2 = CAGDP2[columns]

CAGDP2["GeoArea_FIPS"] = CAGDP2["GeoArea_FIPS"].str.lstrip()

CAGDP2.set_index("GeoArea_FIPS", inplace=True)

# In[ ]:


CAGDP2.to_csv("./Updates/OH_GDP_Data.txt", sep="\t")
