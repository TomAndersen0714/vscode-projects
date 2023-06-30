#!/usr/bin/env bash

cd /data2/code_workplace/data_receiver_docker/

mkdir -p conf/pdd_conf/reminder_pdd_rmd_order

mkdir -p src/rawdata_parser/pdd_parser

cp /opt/bigdata/gitlab/online/20230629/reminder_pdd_rmd_order/docker_run.sh conf/pdd_conf/reminder_pdd_rmd_order/docker_run.sh

cp /opt/bigdata/gitlab/online/20230629/reminder_pdd_rmd_order/reminder_pdd_rmd_order_clickhouse.json conf/pdd_conf/reminder_pdd_rmd_order/reminder_pdd_rmd_order_clickhouse.json

cp /opt/bigdata/gitlab/online/20230629/reminder_pdd_rmd_order/reminder_pdd_rmd_order_clickhouse_parser.py src/rawdata_parser/pdd_parser/reminder_pdd_rmd_order_clickhouse_parser.py

sh /data2/code_workplace/data_receiver_docker/conf/pdd_conf/reminder_pdd_rmd_order/docker_run.sh base.v2.8