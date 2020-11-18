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


df_backup = pd.read_csv(
    "./Updates/STG_ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t"
)
df_backup.to_csv(
    "./Backups/STG_ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries_BACKUP.txt", sep="\t"
)


# In[ ]:


# Read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv"
)


# In[ ]:


# Filter to NC
df_nc = df[df["State"] == "NC"]


# In[ ]:


# Fill MunicipalCodeFIPS to 3 digits
df_nc.loc[:, "MunicipalCodeFIPS"] = df_nc["MunicipalCodeFIPS"].astype(str)
df_nc.loc[:, "MunicipalCodeFIPS"] = df_nc["MunicipalCodeFIPS"].str.zfill(3)


# In[ ]:


# Set index
df_nc.set_index("RegionName", inplace=True)

# Save to txt file
df_nc.to_csv("./Updates/STG_ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t")
