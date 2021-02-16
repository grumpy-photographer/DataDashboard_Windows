from DataDashboard_Windows.Modules import housing

dictionary = {
    "All Homes Top Tier Time Series": "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv",
    "All Homes Bottom Tier Time Series": "http://files.zillowstatic.com/research/public_v2/zhvi/County_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_mon.csv",
}

housing(file_dict=dictionary)