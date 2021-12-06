cd  /home/worker/xiaoduo_bigdata/little_scripts/lark_user 
mongoexport  --host 10.20.131.195  -d growth -c lark_user -o  user.json
mongoexport  --host 10.20.131.195  -d growth -c lark_dept -o  dept.json

python3 user.py
impala-shell -q "DELETE FROM dim.rde_user"
python3 load.py
impala-shell -q "INSERT overwrite dim.rde_user_parq SELECT * FROM dim.rde_user"