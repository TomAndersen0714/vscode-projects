-- MySQL, 10.20.2.130:3306, root, mypass

-- alter table fishpond_task_stat drop column platform;
alter table app_fishpond.fishpond_task_stat
add platform varchar(100) default 'jd' not null after shop_id;

-- drop index fishpond_task_stat_platform_index on app_fishpond.fishpond_task_stat;
create index fishpond_task_stat_platform_index
on app_fishpond.fishpond_task_stat (platform);


-- ClickHouse
DROP TABLE app_fishpond.fishpond_task_stat ON CLUSTER cluster_3s_2r NO DELAY;

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
)
ENGINE = MySQL('10.20.2.130:3306','app_fishpond','fishpond_task_stat','root','mypass');