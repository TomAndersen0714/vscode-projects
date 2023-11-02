cd /data2/code_workplace/data_receiver_docker/

cp src/rawdata_parser/jd_parser/zhike/fishpond_conversion_parser.py /tmp/data_receiver/fishpond_conversion_parser.py-20231101

cp /opt/bigdata/gitlab/online/20231101/fishpond_conversion_parser.py src/rawdata_parser/jd_parser/zhike/fishpond_conversion_parser.py

sh conf/jd_conf/zhike/fishpond_conversion_clickhouse/docker_run_ch.sh base.v2.8