py -W ignore Scripts\CAINC5N_NC.py
py -W ignore Scripts\CAINC6N_NC.py
py -W ignore Scripts\CAINC5N_OH.py
py -W ignore Scripts\CAINC6N_OH.py
py -W ignore Scripts\GeoFRED_Civilian_Labor_Force_by_County_Persons.py
py -W ignore Scripts\GeoFRED_People_25_Years_and_Over_Who_Have_Completed_an_Associates_Degree_or_Higher_5year_estimate_by_County_Percent.py
py -W ignore Scripts\GeoFRED_Resident_Population_by_County_Thousands_of_Persons.py
py -W ignore Scripts\GeoFRED_Unemployment_Rate_by_County_Percent.py

cd Updates
git status

git add *
git commit -a -m "Labor Update %Date%"
git status

git push

cd..