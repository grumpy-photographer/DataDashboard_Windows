"""
Publish DataDashboard data to database
"""

from glob import glob
import urllib
import pandas as pd
from sqlalchemy import create_engine

params = urllib.parse.quote_plus(
    r"Driver={SQL Server Native Client 11.0};"
    r"Server=localhost;"
    r"Database=DataDashboard;"
    r"Trusted_Connection=yes;"
    r"fast_executemany=True;"
)

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

source = glob("./*/Updates/*.txt")

for file in source:
    df = pd.read_csv(file, sep="\t")
    df["Region Code"] = df["Region Code"].astype(str)
    df["Region Code"] = df["Region Code"].str.zfill(5)
    if "Demographics" in file:
        filename = file[23:].replace(".txt", "")
    elif "Labor" in file:
        filename = file[16:].replace(".txt", "")
    elif "Earnings" in file:
        filename = file[19:].replace(".txt", "")
    elif "Land" in file:
        filename = file[15:].replace(".txt", "")
    elif "GDP" in file:
        filename = file[14:].replace(".txt", "")
    elif "Health" in file:
        filename = file[17:].replace(".txt", "")
    print("Uploading " + filename)
    df.to_sql(filename, engine, if_exists="replace", index=False)
