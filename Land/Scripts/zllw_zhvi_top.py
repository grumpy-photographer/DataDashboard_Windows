# Imports
import pandas as pd

# Read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv"
)

# fill state code fips to 2 places
df["StateCodeFIPS"] = df["StateCodeFIPS"].astype(str)
df["StateCodeFIPS"] = df["StateCodeFIPS"].str.zfill(2)

# fill municipal code fips to 3 places
df["MunicipalCodeFIPS"] = df["MunicipalCodeFIPS"].astype(str)
df["MunicipalCodeFIPS"] = df["MunicipalCodeFIPS"].str.zfill(3)

df["Region Code"] = df["StateCodeFIPS"] + df["MunicipalCodeFIPS"]

# drop junk
cols_to_drop = ["RegionID", "SizeRank", "RegionType",
                "State", "Metro", "StateCodeFIPS", "MunicipalCodeFIPS"]
df = df.drop(cols_to_drop, axis=1)

# create measure column
df["Measure Name"] = "All Homes Top Tier Time Series"

# split region name
df["RegionName"] = df["RegionName"].astype(str)
df = df.rename(columns={"RegionName": "Region Name",
                        "StateName": "State Abbreviation"})

# Set index
df.set_index("Region Code", inplace=True)

# Save to txt file
df.to_csv("./Updates/ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t")
