python -W ignore Scripts\zllw_zhvi_top.py
python -W ignore Scripts\zllw_zhvi_bottom.py
python -W ignore Scripts\zllw_msp_smooth.py
python -W ignore Scripts\zllw_msp_raw.py
python -W ignore Scripts\fred_house_transactions.py
python -W ignore Scripts\fred_homeownership.py
python -W ignore Scripts\fred_private_housing.py

git status

git add *

git commit -a -m "Land Update %date%"
git push