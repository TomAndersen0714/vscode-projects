DROP TABLE app_fishpond.fishpond_task_stat ON CLUSTER cluster_3s_2r;

CREATE TABLE app_fishpond.fishpond_task_stat ON CLUSTER cluster_3s_2r
(
    `task_id` String,
    `shop_id` String,
    `platform` String,
    `send_uv` Int32,
    `reply_uv` Int32,
    `created_uv` Int32,
    `created_pv` Int32,
    `created_payment` Float64,
    `paid_uv` Int32,
    `paid_pv` Int32,
    `paid_payment` Float64
) ENGINE = MySQL('10.20.2.130:3306','app_fishpond','fishpond_task_stat','root','mypass');