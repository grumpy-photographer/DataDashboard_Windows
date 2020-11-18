#!/usr/bin/env python
# coding: utf-8

# In[6]:


# Imports
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import urllib
import numpy as np


# In[7]:


# create backups
df_backup = pd.read_csv(
    "./Updates/STG_ZLLW_Metro_MedianSalePrice_AllHomes_Smooth.txt", sep="\t"
)
df_backup.to_csv(
    "./Backups/STG_ZLLW_Metro_MedianSalePrice_AllHomes_Smooth.txt", sep="\t"
)


# In[8]:


# read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/median_sale_price/Metro_median_sale_price_uc_SFRCondo_smoothed_month.csv"
)


# In[9]:


# Filter to NC
df_nc = df[df["StateName"] == "NC"]


# In[10]:


# set index
df_nc.set_index("RegionName", inplace=True)

# save as text file
df_nc.to_csv("./Updates/STG_ZLLW_Metro_MedianSalePrice_AllHomes_Smooth.txt", sep="\t")
