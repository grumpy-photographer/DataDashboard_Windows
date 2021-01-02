python -W ignore Scripts\County_ZHVI_AllHomes_TopTier_TimeSeries.py
python -W ignore Scripts\County_ZHVI_AllHomes_BottomTier_TimeSeries.py
python -W ignore Scripts\Metro_MedianSalePrice_AllHomes_Smooth.py
python -W ignore Scripts\Metro_MedianSalePrice_AllHomes_Raw.py
python -W ignore Scripts\GeoFRED_All_Transactions_House_Price_Index.py
python -W ignore Scripts\GeoFRED_Homeownership_Rate_by_County.py
python -W ignore Scripts\GeoFRED_New_Private_Housing_Structures.py

cd Updates
git status

git add *

git commit -a -m "Land Update %date%"
git status

git push

cd..