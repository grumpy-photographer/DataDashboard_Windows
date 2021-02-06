py -W ignore Scripts\CAINC5N_NC.py
py -W ignore Scripts\CAINC6N_NC.py
py -W ignore Scripts\CAINC5N_OH.py
py -W ignore Scripts\CAINC6N_OH.py
py -W ignore Scripts\fred_civilian_labor_force.py
py -W ignore Scripts\fred_higher_education.py
py -W ignore Scripts\fred_resident_population.py
py -W ignore Scripts\fred_unemployment.py

cd Updates

git add *
git commit -a -m "Labor Update %Date%"
git status

git push

cd..