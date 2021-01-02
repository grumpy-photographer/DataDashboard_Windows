# Imports
import pandas as pd

df_backup = pd.read_csv(
    "./Updates/ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t"
)
df_backup.to_csv(
    "./Backups/ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries_BACKUP.txt", sep="\t"
)

# Read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv"
)

# Fill MunicipalCodeFIPS to 3 digits
df.loc[:, "MunicipalCodeFIPS"] = df["MunicipalCodeFIPS"].astype(str)
df.loc[:, "MunicipalCodeFIPS"] = df["MunicipalCodeFIPS"].str.zfill(3)

# Set index
df.set_index("RegionName", inplace=True)

# Save to txt file
df.to_csv("./Updates/ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t")
