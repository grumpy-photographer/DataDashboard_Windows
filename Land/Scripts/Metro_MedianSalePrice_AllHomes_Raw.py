#!/usr/bin/env python
# coding: utf-8

# In[4]:


# Imports
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np


# In[5]:


# create backups
df_backup = pd.read_csv(
    "./Updates/STG_ZLLW_Metro_MedianSalePrice_AllHomes_Raw.txt", sep="\t"
)
df_backup.to_csv("./Backups/STG_ZLLW_Metro_MedianSalePrice_AllHomes_Raw.txt", sep="\t")


# In[6]:


# read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/median_sale_price/Metro_median_sale_price_uc_SFRCondo_raw_month.csv"
)


# In[7]:


# Filter to NC
df_nc = df[df["StateName"] == "NC"]


# In[9]:


# set index
df_nc.set_index("RegionName", inplace=True)

# save as text file
df_nc.to_csv("./Updates/STG_ZLLW_Metro_MedianSalePrice_AllHomes_Raw.txt", sep="\t")
