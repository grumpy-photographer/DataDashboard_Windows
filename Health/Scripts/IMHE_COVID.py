import os
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
import requests

response = requests.get("https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip")
zip = ZipFile(BytesIO(response.content))
files = zip.namelist()
with zip.open(files[0]) as csvfile:
    df = pd.read_csv('Hospitalization_all_locs.csv')

    print(df)