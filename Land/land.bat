python -W ignore Scripts\County_MedianListingPrice_AllHomes.py
python -W ignore Scripts\County_MedianListingPricePerSqft_AllHomes.py
python -W ignore Scripts\County_MedianValuePerSqft_AllHomes.py
python -W ignore Scripts\County_Zhvi_AllHomes.py
python -W ignore Scripts\GeoFRED_All_Transactions_House_Price_Index.py
python -W ignore Scripts\GeoFRED_Homeownership_Rate_by_County.py
python -W ignore Scripts\GeoFRED_New_Private_Housing_Structures.py

cd Updates
git status

git commit -a -m "Land Update %date%"
git status

git push.simple

cd..