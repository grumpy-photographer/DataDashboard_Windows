py -W ignore Scripts\fred_civilian_labor_force.py
py -W ignore Scripts\fred_eqfx.py
py -W ignore Scripts\fred_higher_education.py
py -W ignore Scripts\fred_resident_population.py

git add *
git commit -a -m "Demographics Update %Date%"

git push