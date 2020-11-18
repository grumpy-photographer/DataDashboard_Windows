#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np


# In[ ]:


# Create backups
df_backup = pd.read_csv("./Updates/STG_FRED_All_Transactions_House_Price_Index.txt")
df_backup.to_csv("./Backups/STG_FRED_All_Transactions_House_Price_Index_BACKUP.txt")


# In[ ]:


# Getting and reading new data
df = pd.read_excel(
    "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=942&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Index+2000%3D100%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1975-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1253&hideLegend=false",
    skiprows=1,
)
df.head(2)


# In[ ]:


# Filter data to display only North Carolina
filter1 = df["Region Name"].str.contains(", NC")
df_nc = df[filter1]


# In[ ]:


# Set Index to Series ID
df_nc.set_index(df_nc["Series ID"], inplace=True)


# In[ ]:


# Drop Series ID column
df_nc.drop("Series ID", axis=1, inplace=True)


# In[ ]:


# Save file to tab delimited txt for upload to SSMS
df_nc.to_csv("./Updates/STG_FRED_All_Transactions_House_Price_Index.txt", sep="\t")
