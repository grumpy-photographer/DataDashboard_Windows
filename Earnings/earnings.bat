Python -W ignore Scripts/CAINC5N_NC.py
Python -W ignore Scripts/NCDOR_MSALESUSETAX_0001.py
Python -W ignore Scripts/NCDOR_MSALESUSETAX_0002.py
Python -W ignore Scripts/Publish_Earnings_Data_Series.py

cd Updates
git status

git commit -a -m "Earnings Update %Date%"
git status

git push

cd..