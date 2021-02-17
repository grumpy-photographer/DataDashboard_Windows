from modules.geofred import geofred

dictionary = {
    "Homeownership Rate": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=157125&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Rate%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1253&hideLegend=false",
    "All Transactions House Price Index": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=942&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Index+2000%3D100%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1975-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1253&hideLegend=false",
    "New Private Housing Structures Authorized by Building Permits": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-89.96&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=155206&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Units%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=999&mapHeight=1253&hideLegend=false",
}

geofred(file_dict=dictionary, file_location="Land")
