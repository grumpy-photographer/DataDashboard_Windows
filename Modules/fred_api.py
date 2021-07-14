from unicodedata import category
import requests
import api_params
import pandas as pd

# Get releases
releases = requests.get(
    (
        "https://api.stlouisfed.org/fred/releases?api_key="
        + api_params.fred_key
        + "&file_type=json"
    )
)

release = pd.read_json(releases.text)

print(release.head())

print("\n\n")

# release 59 #
# category 116 #

# Get Category
categories = requests.get(
    (
        "https://api.stlouisfed.org/fred/category?category_id=112&api_key="
        + api_params.fred_key
        + "&file_type=json"
    )
)

print(categories.text)

#category = pd.read_json(categories.text)

#print(category.head())
