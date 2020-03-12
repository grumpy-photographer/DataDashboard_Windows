python -m pip install --upgrade pip --user
pip install jupyter --upgrade --user
pip install jupyter lab --upgrade --user
pip install pandas --upgrade --user
pip install tensorflow --upgrade
pip install Requests --upgrade
pip install watermark --upgrade
pip install xlrd --upgrade
pip install pyodbc --upgrade
pip install sqlalchemy --upgrade
pip install numpy --upgrade

git config --global push.default matching

cls 

call DataUpdate.py

git status
git commit -a -m "NC Data Dashboard Data Update %date%"
git push
git status
pause