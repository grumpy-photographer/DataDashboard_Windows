import pandas as pd

# create backups
df_backup = pd.read_csv("./Updates/NYTI_CNTY_COVID_19_Deaths.txt", sep="\t")
df_backup.to_csv("./Backups/NYTI_CNTY_COVID_19_Deaths_BACKUP.txt", sep="\t")

# read data
df = pd.read_csv("./Data/covid-19-data/us-counties.csv")

# clean
df = df.rename(
    columns={
        "fips": "GeoArea_FIPS",
        "county": "GeoArea_Name",
        "deaths": "Estimated_Value",
        "date": "Data_Period_Business_Key",
    }
)
df = df.drop(["cases", "state"], axis=1)

df["Data_Period_Business_Key"] = pd.to_datetime(df["Data_Period_Business_Key"])
df["Estimated_Value"] = df["Estimated_Value"].astype(float)

# reset columns
columns = [
    "GeoArea_FIPS",
    "GeoArea_Name",
    "Data_Period_Business_Key",
    "Estimated_Value",
]
df = df[columns]
df.set_index("GeoArea_FIPS", inplace=True)

# save as txt
df.to_csv("./Updates/NYTI_CNTY_COVID_19_Deaths.txt", sep="\t")
