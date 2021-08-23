from zillow import zillow

dictionary = {
    "All Homes Top Tier Time Series": "https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv?t=1629684683",
    "All Homes Bottom Tier Time Series": "https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv?t=1629684683",
}

zillow(file_dict=dictionary, file_location="Land")
