import pandas as pd

# read data
df = pd.read_csv("./Data/covid-19-data/us-counties.csv")

# clean
df = df.rename(
    columns={
        "fips": "Region Code",
        "county": "Region Name",
        "cases": "Estimated Value",
        "date": "Date",
    }
)

df["Region Name"] = df["Region Name"] + ", " + df["state"]
df = df.drop(["deaths", "state"], axis=1)

df["Date"] = pd.to_datetime(df["Date"])
df["Estimated Value"] = df["Estimated Value"].astype(float)
df["Measure Name"] = "COVID Cases"

# reset columns
columns = [
    "Region Code",
    "Region Name",
    "Measure Name",
    "Date",
    "Estimated Value",
]
df = df[columns]
df["Region Code"] = df["Region Code"].astype(str)
df["Region Code"] = df["Region Code"].str.rstrip(".0")
df["Region Code"] = df["Region Code"].str.zfill(5)
df.set_index("Region Code", inplace=True)

# save as txt
df.to_csv("./Updates/NYTI_CNTY_COVID_19_Cases.txt", sep="\t")
