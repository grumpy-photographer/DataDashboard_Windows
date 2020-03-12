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

cd Land
call land.bat
cd..

cls

cd Labor
call labor.bat
cd..

cls

cd Earnings
call earnings.bat
cd..

cls

cd Demographics
call Demographics.bat
cd..

cls

REM cd Health
REM start health.bat
REM cd..

cls

REM cd Natural Products
REM start natproducts.bat
REM cd..

cls

git status

pause
exit
