alter table
    aml_f_product_evaluate_task_calculate_record
add
    column current_remain_risk VARCHAR(20) set utf8mb4 collate utf8mb4_bin DEFAULT NULL COMMENT '当前剩余风险';