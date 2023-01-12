#!/bin/bash
cd /home/worker/xiaoduo_bigdata/little_scripts


python3 refresh_impala_schema.py
python3 load_schemas.py
