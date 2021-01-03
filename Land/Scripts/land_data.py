import pandas as pd

# create empty dataframe
df = pd.DataFrame()

# read house price data
df2 = pd.read_csv(
    "./Updates/FRED_All_Transactions_House_Price_Index_by_County.txt", sep="\t")

# read homeownership data
df3 = pd.read_csv("./Updates/FRED_Homeownership_Rate_by_County.txt", sep="\t")

# read housing structures data
df4 = pd.read_csv(
    "./Updates/FRED_New_Private_Housing_Structures_Authorized_by_Building_Permits_by_County.txt", sep="\t")

# read bottom tier home sales data
df5 = pd.read_csv(
    "./Updates/ZLLW_County_ZHVI_AllHomes_BottomTier_TimeSeries.txt", sep="\t")

# read top tier home sales data
df6 = pd.read_csv(
    "./Updates/ZLLW_County_ZHVI_AllHomes_TopTier_TimeSeries.txt", sep="\t")

# append data
df_list = [df2, df3, df4, df5, df6]
data = df.append(df_list)

# make sure region code is str and set index
data["Region Code"] = data["Region Code"].astype(str)
data["Region Code"] = data["Region Code"].str.zfill(5)
data = data.set_index("Region Code")

# save to text file
data.to_csv("./Updates/land.txt", sep="\t")
