关联工单：https://project.feishu.cn/dev_demands/story/detail/15739849

登录用户：root，登录节点：jstzjk-002130-prod-tb-bigdata-bigdata，登录集群：ClickHouse
操作命令：
执行DDL文件中的命令

登录用户：worker，登录节点：jstzjk-2131-prod-tb-docker-bigdata
操作命令：
备份/home/worker/airflow/dags/xqc_stat_tb.py文件到对应路径下backup文件夹，并使用20231012作为后缀
使用/opt/bigdata/gitlab/online/20231012/xqc_stat_tb.py文件，替换/home/worker/airflow/dags/xqc_stat_tb.py

登录用户：root，登录节点：mini-bigdata-004，登录集群：ClickHouse
操作命令：
执行DDL文件中的命令

登录用户：worker，登录节点：v1mini-bigdata-002
操作命令：
备份/home/worker/airflow/dags/xqc_stat_mini.py文件到对应路径下backup文件夹，并使用20231012作为后缀
使用/opt/bigdata/gitlab/online/20231012/xqc_stat_mini.py文件，替换/home/worker/airflow/dags/xqc_stat_mini.py