rm lambda-weekly-refresh.zip
cd env/lib/python2.7/site-packages/
zip -r9 ../../../../lambda-weekly-refresh.zip *
cd ../../../../
zip -g lambda-weekly-refresh.zip weekly_refresh.py
zip -r9 lambda-weekly-refresh.zip utilities
