# Imports
import pandas as pd

# Read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv"
)

# fill state code fips to 2 places
df["State Code FIPS"] = df["State Code FIPS"].astype(str)
df["State Code FIPS"] = df["State Code FIPS"].str.zfill(2)

# fill municipal code fips to 3 places
df["Municipal Code FIPS"] = df["Municipal Code FIPS"].astype(str)
df["Municipal Code FIPS"] = df["Municipal Code FIPS"].str.zfill(3)

df["Region Code"] = df["State Code FIPS"] + df["Municipal Code FIPS"]

# drop junk
cols_to_drop = ["Region ID", "Size Rank", "Region Type",
                "State", "Metro", "State Code FIPS", "Municipal Code FIPS"]
df = df.drop(cols_to_drop)

# create measure column
df["Measure Name"] = "All Homes Top Tier Time Series"

# Set index
df.set_index("RegionName", inplace=True)

# Save to txt file
df.to_csv("./Updates/ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t")
