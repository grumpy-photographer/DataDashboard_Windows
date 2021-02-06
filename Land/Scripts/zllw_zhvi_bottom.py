# Imports
import pandas as pd

# Read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_mon.csv"
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
df["Measure Name"] = "All Homes Bottom Tier Time Series"

# split region name
df["RegionName"] = df["RegionName"].astype(str)
df = df.rename(columns={"RegionName": "Region Name"})
df["Region Name"] = df["Region Name"] + ", " + df["StateName"]
df.drop("StateName", axis=1, inplace=True)

# Melt df
df = df.melt(id_vars=["Region Code", "Region Name", "Measure Name"],
             value_name = "Estimated Value", var_name="Date")
df.set_index("Region Code", inplace=True)
df["Date"] = pd.to_datetime(df["Date"])

# Save to txt file
df.to_csv(
    "./Updates/ZLLW_County_ZHVI_AllHomes_BottomTier_TimeSeries.txt", sep="\t"
)
