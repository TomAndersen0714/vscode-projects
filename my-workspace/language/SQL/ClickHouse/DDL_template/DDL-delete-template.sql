ALTER TABLE dwd.voc_cnick_list_local ON CLUSTER cluster_3s_2r
DELETE IN PARTITION 20230803 WHERE platform IN ['ks', 'dy']