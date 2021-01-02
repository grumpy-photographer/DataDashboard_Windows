# Imports
import pandas as pd

# read data
df = pd.read_csv(
    "http://files.zillowstatic.com/research/public_v2/median_sale_price/Metro_median_sale_price_uc_SFRCondo_smoothed_month.csv"
)

# set index
df.set_index("RegionName", inplace=True)

# save as text file
df.to_csv("./Updates/ZLLW_Metro_MedianSalePrice_AllHomes_Smooth.txt", sep="\t")
