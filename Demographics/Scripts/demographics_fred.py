from ..Modules.geofred import geofred

dictionary = {
    "Civilian Labor Force": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90&lat=40&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=656&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Persons%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1990-01-01&endDate=2030-01-01&mapWidth=1128&mapHeight=1063&hideLegend=false",
    "EQFXSUBPRIME": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=39.98&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147149&attributes=Not+Seasonally+Adjusted%2C+Quarterly%2C+Percent%2C+no_period_desc&aggregationFrequency=Quarterly&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1999-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    "People 25 Years and Over Who Have Completed an Associates Degree or Higher 5 Year Estimate": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=147063&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Percent%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=2009-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
    "Resident Population": "https://geofred.stlouisfed.org/api/download.php?theme=pubugn&colorCount=5&reverseColors=false&intervalMethod=fractile&displayStateOutline=true&lng=-90.00&lat=40.01&zoom=4&showLabels=true&showValues=true&regionType=county&seriesTypeId=1549&attributes=Not+Seasonally+Adjusted%2C+Annual%2C+Thousands+of+Persons%2C+no_period_desc&aggregationFrequency=Annual&aggregationType=Average&transformation=lin&date=2030-01-01&type=xls&startDate=1970-01-01&endDate=2030-01-01&mapWidth=2000&mapHeight=1214&hideLegend=false",
}

geofred(file_dict=dictionary)
