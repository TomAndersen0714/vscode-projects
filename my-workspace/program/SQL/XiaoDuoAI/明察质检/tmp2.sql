ALTER TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r
ADD INDEX idx_min_max_day day TYPE minmax GRANULARITY 24

ALTER TABLE xqc_dim.qc_norm_group_full_local ON CLUSTER cluster_3s_2r
MATERIALIZE INDEX idx_min_max_day