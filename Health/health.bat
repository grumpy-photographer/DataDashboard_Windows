cd Data
cd covid-19-Data
git fetch
git pull

cd..
cd..

python Scripts\COVID_Cases.py
python Scripts\COVID_Deaths.py

cd Updates
git status

git commit -a -m "COVID Update %date%"
git status

git push

cd..