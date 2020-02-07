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

git config --global user.name "nathayoung"
git config --global push.default matching

cd Land
call land.bat
cd..

cd Labor
call labor.bat
cd..

cd Earnings
call earnings.bat
cd..

cd Demographics
call Demographics.bat
cd..

REM cd Health
REM start health.bat
REM cd..

REM cd Natural Products
REM start natproducts.bat
REM cd..

git status

pause
exit
