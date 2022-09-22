import logging as log
import time

from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.clickhouse_executor import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator

# MONGO_CONN_ID = "xdqc_mongo_mini_test"
# CLICKHOUSE_CONN_ID = 'clickhouse_mini_test'
MONGO_CONN_ID = "xdqc_offline"
CLICKHOUSE_CONN_ID = "clickhouse_zjk_008"
PLATFORMS = ["jd", "pdd", "wx", "ks", "dy", "open"]
CH_HOOK = None

args = {
    'owner': 'chenhao03',
    'start_date': datetime(2022, 9, 22),
    'email': ['chenhao03@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='xqc_stat_another_tb',
    schedule_interval="30 9 * * *",
    max_active_runs=1,
    default_args=args
)


def get_ch_hook() -> ClickHouseHook:
    global CH_HOOK
    if CH_HOOK is None:
        CH_HOOK = ClickHouseHook(CLICKHOUSE_CONN_ID)
    return CH_HOOK


def ch_execute(sql: str, sleep_sec: int = 1):
    ch_hook = get_ch_hook()

    log.info(sql)
    res = ch_hook.execute(sql)
    time.sleep(sleep_sec)

    return res


def truncate_ch_tmp_table_func():
    ch_execute(f"truncate table xqc_dwd.wiped_tag_local ON CLUSTER cluster_3s_2r")
    ch_execute(f"truncate table xqc_dwd.manual_task_record_local ON CLUSTER cluster_3s_2r")
    ch_execute(f"truncate table xqc_dwd.manual_task_local ON CLUSTER cluster_3s_2r")


dwd_manual_task_record_all = ClickHouseOperator(
    task_id='dwd_manual_task_record_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    sql="""
    INSERT INTO xqc_dwd.manual_task_record_all
    SELECT task._id,
           task.create_time,
           task.update_time,
           task.company_id,
           customer.name AS company_name,
           task.platform,
           task.day,
           task.date,
           task.task_name,
           task.task_id,
           task.qc_type,
           task.qc_way,
           task.account_name,
           task.qc_norm_id,
           task.target_num,
           task.mark_num,
           task.ontime_mark_num,
           task.overdue_mark_num,
           task.ai_num,
           task.ai_rate,
           task.human_num,
           task.human_rate
    FROM xqc_ods.manual_task_record_all task
    LEFT JOIN
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE day = toYYYYMMDD(yesterday()) ) customer ON customer._id = task.company_id
    """,
    dag=dag
)

ods_dwd_manual_task_all = ClickHouseOperator(
    task_id='ods_dwd_manual_task_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    sql="""
    INSERT INTO xqc_dwd.manual_task_all
    SELECT task._id as _id,
        task.create_time as create_time,
        task.update_time as update_time,
        task.platform as platform,
        task.company_id as company_id,
        customer.name as company_name,
        task.creator as creator,
        task.name as name,
        task.type as type,
        task.account_id as account_id,
        task.account_name as account_name,
        task.cycle_strategy as cycle_strategy,
        task.cycle_date_gte as cycle_date_gte,
        task.cycle_date_lte as cycle_date_lte,
        task.dialog_date as dialog_date,
        task.dialog_date_range_gte as dialog_date_range_gte,
        task.dialog_date_range_lte as dialog_date_range_lte,
        task.task_grade as task_grade,
        task.qc_way as qc_way,
        task.target_num as target_num,
        task.employee_ids as employee_ids,
        task.real_num as real_num,
        task.each_num as each_num
    FROM xqc_ods.manual_task_all task
    LEFT JOIN
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE day = toYYYYMMDD(yesterday()) ) customer ON customer._id = task.company_id
    """,
    dag=dag
)

dwd_wiped_tag_all = ClickHouseOperator(
    task_id='dwd_wiped_tag_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_dwd.wiped_tag_all
    SELECT toYYYYMMDD(toDate(task.wipe_time)) AS day,
           task.company_id,
           customer.name AS company_name,
           task.platform,
           shop.shop_id AS shop_id,
           shop.plat_shop_name AS shop_name,
           task.seller_nick,
           task.snick,
           task.cnick,
           task.wipe_id,
           task.wipe_time,
           task.dialog_id,
           task.dialog_time,
           task.messages_id,
           task.abnormal_types,
           task.abnormal_scores,
           task.excellents_types,
           task.excellents_scores,
           task.rule_scores_ids,
           task.rule_scores_counts,
           task.rule_scores_scores,
           task.emotion_type,
           task.emotion_score
    FROM xqc_ods.wiped_tag_all task
    LEFT JOIN
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE day = toYYYYMMDD(yesterday()) ) customer ON customer._id = task.company_id
    LEFT JOIN
      (SELECT *
       FROM xqc_dim.xqc_shop_all
       WHERE day = toYYYYMMDD(yesterday()) ) shop ON shop.seller_nick = task.seller_nick
    AND shop.platform = task.platform
"""
)

truncate_ch_tmp_table = PythonOperator(
    task_id="truncate_ch_dwd_qc_tmp_table",
    python_callable=truncate_ch_tmp_table_func,
    dag=dag
)

xqc_dws_alert_tag_stat_all = ClickHouseOperator(
    task_id='xqc_dws_alert_tag_stat_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_dws.alert_tag_stat_all
    WITH base_t AS
      (SELECT platform,
              `day`,
              `level`,
              '' AS warning_tag_id,
              warning_type AS warning_tag_name,
              shop_id,
              count(1) AS warning_cnt,
              sum(if(is_finished = 'True', 1, 0)) AS finished_warning_cnt,
              sum(if(is_finished = 'True', dateDiff('minute',toDateTime(`time`), toDateTime(if(finish_time != '', finish_time, NULL))), 0)) AS finish_elapsed_time
       FROM xqc_ods.alert_all
       WHERE `day` = {{ ds_nodash }}
       GROUP BY platform,
                `day`,
                `level`,
                warning_type,
                shop_id),
         customer AS
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         shop AS
      (SELECT distinct(seller_nick, platform),
              shop_id,
              seller_nick,
              platform,
              company_id,
              plat_shop_name
       FROM xqc_dim.xqc_shop_all
       WHERE `day` = toYYYYMMDD(yesterday()) )
    SELECT base_t.`day` AS `day`,
           shop.company_id AS company_id,
           customer.name AS company_name,
           base_t.platform AS platform,
           base_t.shop_id AS shop_id,
           shop.plat_shop_name AS shop_name,
           shop.seller_nick AS seller_nick,
           base_t.level AS `level`,
           base_t.warning_tag_id as warning_tag_id,
           base_t.warning_tag_name AS warning_tag_name,
           base_t.warning_cnt AS warning_cnt,
           base_t.finished_warning_cnt AS finished_warning_cnt,
           base_t.finish_elapsed_time AS finish_elapsed_time
    FROM base_t
    LEFT JOIN shop ON shop.shop_id = base_t.shop_id
    LEFT JOIN customer ON customer._id = shop.company_id
    """
)

xqc_dws_qc_norm_stat_all = ClickHouseOperator(
    task_id='xqc_dws_qc_norm_stat_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_dws.qc_norm_stat_all
    WITH qc_norm AS
      (SELECT *
       FROM ods.xinghuan_qc_norm_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         customer AS
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         shop AS
      (SELECT distinct(seller_nick, platform),
              shop_id,
              seller_nick,
              platform,
              plat_shop_name
       FROM xqc_dim.xqc_shop_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         rule_all AS
      (SELECT *
       FROM xqc_dim.qc_rule_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         group_info AS
      (SELECT *
       FROM xqc_dim.qc_norm_group_full_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         tags AS
      (SELECT *
       FROM xqc_dws.tag_stat_all
       WHERE `day` = {{ ds_nodash }} ),
         qc_base_v2 AS
      (SELECT qc_norm._id AS qc_norm_id,
              qc_norm.company_id AS company_id,
              qc_norm.name AS qc_norm_name,
              rule_all._id AS qc_rule_id,
              rule_all.name AS qc_rule_name,
              rule_all.rule_category AS rule_category,
              rule_all.rule_type AS rule_type,
              rule_all.check AS is_check,
              rule_all.status AS status,
              rule_all.alert_level AS alert_level,
              rule_all.notify_way AS notify_way,
              rule_all.notify_target AS notify_target,
              rule_all.qc_norm_group_id AS qc_norm_group_id
       FROM qc_norm
       LEFT JOIN rule_all ON rule_all.qc_norm_id = qc_norm._id),
         qc_base_v3 AS
      (SELECT qc_base_v2.*,
              group_info.name AS qc_norm_group_name,
              group_info.full_name AS qc_norm_group_full_name
       FROM qc_base_v2
       LEFT JOIN group_info ON group_info._id = qc_base_v2.qc_norm_group_id),
         qc AS
      (SELECT tags.day AS `day`,
              qc_base_v3.company_id AS company_id,
              customer.name AS company_name,
              tags.platform AS platform,
              shop.shop_id AS shop_id,
              shop.plat_shop_name AS shop_name,
              tags.seller_nick AS seller_nick,
              qc_base_v3.qc_norm_group_id AS qc_norm_group_id,
              qc_base_v3.qc_norm_group_name AS qc_norm_group_name,
              qc_base_v3.qc_norm_group_full_name AS qc_norm_group_full_name,
              qc_base_v3.qc_norm_id AS qc_norm_id,
              qc_base_v3.qc_norm_name AS qc_norm_name,
              qc_base_v3.qc_rule_id AS qc_rule_id,
              qc_base_v3.qc_rule_name AS qc_rule_name,
              qc_base_v3.rule_category AS rule_category,
              qc_base_v3.rule_type AS rule_type,
              qc_base_v3.is_check AS is_check,
              qc_base_v3.status AS status,
              qc_base_v3.alert_level AS alert_level,
              qc_base_v3.notify_way AS notify_way,
              qc_base_v3.notify_target AS notify_target,
              tags.tag_cnt_sum AS trigger_cnt
       FROM tags
       LEFT JOIN qc_base_v3 ON tags.tag_id = qc_base_v3.qc_rule_id
       LEFT JOIN customer ON customer._id = qc_base_v3.company_id
       LEFT JOIN shop ON shop.seller_nick = tags.seller_nick
       AND shop.platform = tags.platform)
    SELECT *
    FROM qc
    """
)


def clean_open_stat_partition_func(ds_nodash):
    ds_nodash = int(ds_nodash)
    ch_execute(f"ALTER TABLE xqc_dws.qc_norm_stat_local ON CLUSTER cluster_3s_2r DROP PARTITION {ds_nodash}")
    ch_execute(f"ALTER TABLE xqc_dws.alert_tag_stat_local ON CLUSTER cluster_3s_2r DROP PARTITION {ds_nodash}")
    for platform in PLATFORMS:
        ch_execute(
            f"ALTER TABLE xqc_dws.dialog_eval_stat_local ON CLUSTER cluster_3s_2r "
            f"DROP PARTITION ({ds_nodash}, '{platform}')"
        )
    ch_execute(f"ALTER TABLE xqc_ads.shop_stat_local ON CLUSTER cluster_3s_2r DROP PARTITION {ds_nodash}")
    ch_execute(f"ALTER TABLE xqc_ads.company_stat_local ON CLUSTER cluster_3s_2r DROP PARTITION {ds_nodash}")


clean_open_stat_partition = PythonOperator(
    task_id="clean_open_stat_partition",
    python_callable=clean_open_stat_partition_func,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

xqc_dws_dialog_eval_stat_all = ClickHouseOperator(
    task_id='xqc_dws_dialog_eval_stat_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_dws.dialog_eval_stat_all
    SELECT
        day,
        platform,
        seller_nick,
        snick,
        eval_code,
        COUNT(1) AS eval_cnt,
        COUNT(DISTINCT dialog_id) AS dialog_cnt
    FROM xqc_ods.dialog_eval_all
    WHERE day = toYYYYMMDD(yesterday())
    GROUP BY day, platform, seller_nick, snick, eval_code
    """
)

xqc_ads_shop_stat_all = ClickHouseOperator(
    task_id='xqc_ads_shop_stat_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_ads.shop_stat_all
    WITH customer AS
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         shop_temp AS
      (SELECT distinct(seller_nick, platform),
              shop_id,
              seller_nick,
              platform,
              plat_shop_name,
              company_id,
              `day`
       FROM xqc_dim.xqc_shop_all
       WHERE `day` = toYYYYMMDD(yesterday())),
         shop AS
      (SELECT shop_temp.company_id AS company_id,
              shop_temp.shop_id AS shop_id,
              shop_temp.platform AS platform,
              shop_temp.plat_shop_name AS shop_name,
              shop_temp.seller_nick AS seller_nick,
              shop_temp.day AS `day`,
              customer.name AS company_name
       FROM shop_temp
       LEFT JOIN customer ON shop_temp.company_id = customer._id),
         snick_stat_all AS
      (SELECT seller_nick,
              platform,
              count(DISTINCT snick) AS snick_uv,
              sum(tagged_dialog_cnt) AS tagged_dialog_cnt,
              sum(ai_tagged_dialog_cnt) AS ai_tagged_dialog_cnt,
              sum(custom_tagged_dialog_cnt) AS custom_tagged_dialog_cnt,
              sum(manual_tagged_dialog_cnt) AS manual_tagged_dialog_cnt,
              sum(subtract_score_dialog_cnt) AS subtract_score_dialog_cnt,
              sum(add_score_dialog_cnt) AS add_score_dialog_cnt,
              sum(subtract_score_sum) AS subtract_score_sum,
              sum(subtract_score_sum) AS subtract_score_sum,
              sum(add_score_sum) AS add_score_sum,
              sum(ai_subtract_score_sum) AS ai_subtract_score_sum,
              sum(ai_add_score_sum) AS ai_add_score_sum,
              sum(custom_subtract_score_sum) AS custom_subtract_score_sum,
              sum(custom_add_score_sum) AS custom_add_score_sum,
              sum(manual_subtract_score_sum) AS manual_subtract_score_sum,
              sum(manual_add_score_sum) AS manual_add_score_sum,
              sum(dialog_cnt) AS dialog_cnt,
              sum(manual_marked_dialog_cnt) AS manual_marked_dialog_cnt,
              sum(ai_subtract_score_dialog_cnt) AS ai_subtract_score_dialog_cnt,
              sum(ai_add_score_dialog_cnt) AS ai_add_score_dialog_cnt,
              sum(custom_subtract_score_dialog_cnt) AS custom_subtract_score_dialog_cnt,
              sum(custom_add_score_dialog_cnt) AS custom_add_score_dialog_cnt,
              sum(manual_subtract_score_dialog_cnt) AS manual_subtract_score_dialog_cnt,
              sum(manual_add_score_dialog_cnt) AS manual_add_score_dialog_cnt
       FROM xqc_dws.snick_stat_all
       WHERE `day` = {{ ds_nodash }} 
       GROUP BY seller_nick,
                platform),
         alert AS
      (SELECT seller_nick,
              platform,
              sum(warning_cnt) AS alert_cnt,
              sum(if(`level` = 1, warning_cnt, 0)) AS level_1_alert_cnt,
              sum(if(`level` = 2, warning_cnt, 0)) AS level_2_alert_cnt,
              sum(if(`level` = 3, warning_cnt, 0)) AS level_3_alert_cnt,
              sum(if(`level` = 1, finished_warning_cnt, 0)) AS level_1_alert_finished_cnt,
              sum(if(`level` = 2, finished_warning_cnt, 0)) AS level_2_alert_finished_cnt,
              sum(if(`level` = 3, finished_warning_cnt, 0)) AS level_3_alert_finished_cnt,
              sum(finish_elapsed_time) AS alert_finished_mins
       FROM xqc_dws.alert_tag_stat_all
       WHERE `day` = {{ ds_nodash }} 
       GROUP BY seller_nick,
                platform),
         eval_all AS
      (SELECT seller_nick,
              platform,
              count(seller_nick) AS eval_cnt
       FROM xqc_dws.dialog_eval_stat_all
       WHERE `day` = {{ ds_nodash }} 
       GROUP BY seller_nick,
                platform),
         eval_level AS
      (WITH t AS
         (SELECT seller_nick,
                 platform,
                 eval_code,
                 count(eval_code) AS eval_cnt
          FROM xqc_dws.dialog_eval_stat_all
          WHERE `day` = {{ ds_nodash }} 
          GROUP BY seller_nick,
                   platform,
                   eval_code) SELECT seller_nick,
                                     platform,
                                     groupArray(cast(eval_code AS String)) AS eval_levels,
                                     groupArray(cast(eval_cnt AS int)) AS eval_level_cnts
       FROM t
       GROUP BY seller_nick,
                platform),
         wipe AS
      (SELECT seller_nick,
              platform,
              count(DISTINCT messages_id) AS wiped_tag_cnt,
              sum(length(abnormal_types) + length(excellents_types) + if(emotion_type != '', 1, 0)) AS wiped_ai_tag_cnt,
              0 AS wiped_manual_tag_cnt,
              sum(length(rule_scores_ids)) AS wiped_custom_tag_cnt
       FROM xqc_dwd.wiped_tag_all
       WHERE `day` = toYYYYMMDD(yesterday())
       GROUP BY seller_nick,
                platform),
         dialog_all AS
      ( SELECT count(DISTINCT cnick) AS cnick_uv,
               seller_nick,
               platform
       FROM dwd.xdqc_dialog_all
       WHERE toYYYYMMDD(begin_time) = toYYYYMMDD(yesterday())
       GROUP BY seller_nick,
                platform )
    SELECT {{ ds_nodash }} AS `day`,
           shop.company_id AS company_id,
           shop.company_name AS company_name,
           shop.platform AS platform,
           shop.shop_id AS shop_id,
           shop.shop_name AS shop_name,
           shop.seller_nick AS seller_nick,
           snick_stat_all.snick_uv AS snick_uv,
           dialog_all.cnick_uv AS cnick_uv,
           snick_stat_all.subtract_score_sum AS subtract_score_sum,
           snick_stat_all.add_score_sum AS add_score_sum,
           snick_stat_all.ai_subtract_score_sum AS ai_subtract_score_sum,
           snick_stat_all.ai_add_score_sum AS ai_add_score_sum,
           snick_stat_all.custom_subtract_score_sum AS custom_subtract_score_sum,
           snick_stat_all.custom_add_score_sum AS custom_add_score_sum,
           snick_stat_all.manual_subtract_score_sum AS manual_subtract_score_sum,
           snick_stat_all.manual_add_score_sum AS manual_add_score_sum,
           snick_stat_all.dialog_cnt AS dialog_cnt,
           snick_stat_all.tagged_dialog_cnt AS tagged_dialog_cnt,
           snick_stat_all.ai_tagged_dialog_cnt AS ai_tagged_dialog_cnt,
           snick_stat_all.custom_tagged_dialog_cnt AS custom_tagged_dialog_cnt,
           snick_stat_all.manual_tagged_dialog_cnt AS manual_tagged_dialog_cnt,
           snick_stat_all.subtract_score_dialog_cnt AS subtract_score_dialog_cnt,
           snick_stat_all.add_score_dialog_cnt AS add_score_dialog_cnt,
           snick_stat_all.manual_marked_dialog_cnt AS manual_marked_dialog_cnt,
           snick_stat_all.ai_subtract_score_dialog_cnt AS ai_subtract_score_dialog_cnt,
           snick_stat_all.ai_add_score_dialog_cnt AS ai_add_score_dialog_cnt,
           snick_stat_all.custom_subtract_score_dialog_cnt AS custom_subtract_score_dialog_cnt,
           snick_stat_all.custom_add_score_dialog_cnt AS custom_add_score_dialog_cnt,
           snick_stat_all.manual_subtract_score_dialog_cnt AS manual_subtract_score_dialog_cnt,
           snick_stat_all.manual_add_score_dialog_cnt AS manual_add_score_dialog_cnt,
           alert.alert_cnt AS alert_cnt,
           alert.level_1_alert_cnt AS level_1_alert_cnt,
           alert.level_2_alert_cnt AS level_2_alert_cnt,
           alert.level_3_alert_cnt AS level_3_alert_cnt,
           alert.level_1_alert_finished_cnt AS level_1_alert_finished_cnt,
           alert.level_2_alert_finished_cnt AS level_2_alert_finished_cnt,
           alert.level_3_alert_finished_cnt AS level_3_alert_finished_cnt,
           alert.alert_finished_mins AS alert_finished_mins,
           wipe.wiped_tag_cnt AS wiped_tag_cnt,
           wipe.wiped_ai_tag_cnt AS wiped_ai_tag_cnt,
           wipe.wiped_manual_tag_cnt AS wiped_manual_tag_cnt,
           wipe.wiped_custom_tag_cnt AS wiped_custom_tag_cnt,
           eval_all.eval_cnt AS eval_cnt,
           eval_level.eval_levels AS eval_levels,
           eval_level.eval_level_cnts AS eval_level_cnts
    FROM shop
    LEFT JOIN snick_stat_all ON snick_stat_all.platform = shop.platform
    AND snick_stat_all.seller_nick = shop.seller_nick
    LEFT JOIN alert ON alert.platform = shop.platform
    AND alert.seller_nick = shop.seller_nick
    LEFT JOIN wipe ON wipe.platform = shop.platform
    AND wipe.seller_nick = shop.seller_nick
    LEFT JOIN eval_all ON eval_all.platform = shop.platform
    AND eval_all.seller_nick = shop.seller_nick
    LEFT JOIN eval_level ON eval_level.platform = shop.platform
    AND eval_level.seller_nick = shop.seller_nick
    LEFT JOIN dialog_all ON dialog_all.platform = shop.platform
    AND dialog_all.seller_nick = shop.seller_nick
    """
)

xqc_ads_platform_company_stat_all = ClickHouseOperator(
    task_id='xqc_ads_platform_company_stat_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_ads.platform_company_stat_all
    WITH shop_base AS
      (SELECT *
       FROM xqc_dim.xqc_shop_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         eval_level_t AS
      (WITH t AS
         (SELECT company_id,
                 platform,
                 eval_level,
                 eval_level_cnt
          FROM xqc_ads.shop_stat_all ARRAY
          JOIN eval_levels AS eval_level,
               eval_level_cnts AS eval_level_cnt
          WHERE `day` = {{ ds_nodash }} ) SELECT company_id,
                                                         platform,
                                                         groupArray(cast(eval_level AS String)) AS eval_levels,
                                                         groupArray(cast(eval_level_cnt AS int)) AS eval_level_cnts
       FROM t
       GROUP BY company_id,
                platform),
         customer AS
      (SELECT *
       FROM ods.xinghuan_company_all
       WHERE `day` = toYYYYMMDD(yesterday()) ),
         ads_shop AS
      (SELECT `day`,
              company_id,
              platform,
              count(DISTINCT shop_id) AS shop_cnt,
              sum(snick_uv) AS snick_uv,
              sum(cnick_uv) AS cnick_uv,
              sum(subtract_score_sum) AS subtract_score_sum,
              sum(add_score_sum) AS add_score_sum,
              sum(ai_subtract_score_sum) AS ai_subtract_score_sum,
              sum(ai_add_score_sum) AS ai_add_score_sum,
              sum(custom_subtract_score_sum) AS custom_subtract_score_sum,
              sum(custom_add_score_sum) AS custom_add_score_sum,
              sum(manual_subtract_score_sum) AS manual_subtract_score_sum,
              sum(manual_add_score_sum) AS manual_add_score_sum,
              sum(dialog_cnt) AS dialog_cnt,
              sum(tagged_dialog_cnt) AS tagged_dialog_cnt,
              sum(ai_tagged_dialog_cnt)AS ai_tagged_dialog_cnt,
              sum(custom_tagged_dialog_cnt)AS custom_tagged_dialog_cnt,
              sum(manual_tagged_dialog_cnt)AS manual_tagged_dialog_cnt,
              sum(subtract_score_dialog_cnt)AS subtract_score_dialog_cnt,
              sum(add_score_dialog_cnt)AS add_score_dialog_cnt,
              sum(manual_marked_dialog_cnt) AS manual_marked_dialog_cnt,
              sum(ai_subtract_score_dialog_cnt) AS ai_subtract_score_dialog_cnt,
              sum(ai_add_score_dialog_cnt) AS ai_add_score_dialog_cnt,
              sum(custom_subtract_score_dialog_cnt) AS custom_subtract_score_dialog_cnt,
              sum(custom_add_score_dialog_cnt) AS custom_add_score_dialog_cnt,
              sum(manual_subtract_score_dialog_cnt) AS manual_subtract_score_dialog_cnt,
              sum(manual_add_score_dialog_cnt) AS manual_add_score_dialog_cnt,
              sum(alert_cnt) AS alert_cnt,
              sum(level_1_alert_cnt) AS level_1_alert_cnt,
              sum(level_2_alert_cnt) AS level_2_alert_cnt,
              sum(level_3_alert_cnt) AS level_3_alert_cnt,
              sum(level_1_alert_finished_cnt) AS level_1_alert_finished_cnt,
              sum(level_2_alert_finished_cnt) AS level_2_alert_finished_cnt,
              sum(level_3_alert_finished_cnt) AS level_3_alert_finished_cnt,
              sum(alert_finished_mins) AS alert_finished_mins,
              sum(wiped_tag_cnt) AS wiped_tag_cnt,
              sum(wiped_ai_tag_cnt) AS wiped_ai_tag_cnt,
              sum(wiped_manual_tag_cnt) AS wiped_manual_tag_cnt,
              sum(wiped_custom_tag_cnt) AS wiped_custom_tag_cnt,
              sum(eval_cnt) AS eval_cnt
       FROM xqc_ads.shop_stat_all
       WHERE `day` = {{ ds_nodash }}
       GROUP BY company_id,
                `day`,
                platform),
         qc_norm AS
      (SELECT company_id,
              platform,
              count(1) AS qc_norm_cnt,
              sum(status = 1) AS qc_norm_opened_cnt
       FROM ods.xinghuan_qc_norm_all
       WHERE `day` = toYYYYMMDD(yesterday())
       GROUP BY company_id,
                platform),
         qc_rule AS
      (SELECT company_id,
              platform,
              count(1) AS tag_cnt,
              sum(status = 1) AS open_alert_tag_cnt,
              sum(rule_category = 1) AS ai_tag_cnt,
              sum(rule_category = 1
                  AND status = 1) AS ai_tag_opened_cnt,
              sum(rule_category = 3) AS custom_tag_cnt,
              sum(rule_category = 3
                  AND status = 1) AS custom_tag_opened_cnt,
              sum(rule_category = 2) AS manual_tag_cnt,
              sum(rule_category = 2
                  AND status = 1) AS manual_tag_opened_cnt
       FROM xqc_dim.qc_rule_all
       WHERE `day` = toYYYYMMDD(yesterday())
         AND status!=2
       GROUP BY company_id,
                platform),
         qc_word AS
      (SELECT company_id,
              platform,
              count(1) AS qc_word_cnt,
              sum(check_custom = 'True'
                  OR check_service = 'True') AS qc_word_opened_cnt
       FROM xqc_dim.qc_word_setting_all
       WHERE `day` = toYYYYMMDD(yesterday())
       GROUP BY company_id,
                platform),
         appeal_task AS
      (SELECT company_id,
              platform,
              count(1) AS appeal_task_cnt
       FROM xqc_ods.xqc_appeal_task_all
       GROUP BY company_id,
                platform),
         qt_task AS
      (SELECT company_id,
              platform,
              count(1) AS qt_task_cnt
       FROM xqc_ods.xinghuan_mc_qt_task_all
       GROUP BY company_id,
                platform),
         mc_case AS
      (SELECT company_id,
              platform,
              count(1) AS dialog_case_cnt
       FROM xqc_ods.xinghuan_mc_case_detail_all
       WHERE toDate(left(create_time,9)) = toDate(yesterday())
       GROUP BY company_id,
                platform),
         operation_log AS
      (SELECT company_id,
              platform,
              count(1) AS qc_norm_edit_cnt
       FROM xqc_ods.qc_norm_operation_log_all
       WHERE `day` = toYYYYMMDD(yesterday())
         AND toDate(create_time) = toDate(yesterday())
       GROUP BY company_id,
                platform),
         web_log_all AS
      (SELECT uniqExact(arrayElement(splitByString(':', distinct_id),2)) AS uv,
              count(1) AS pv,
              arrayElement(splitByString(':', distinct_id),1) AS shot_name,
              platform,
              count(DISTINCT distinct_id) AS account_uv,
              'True' AS is_active
       FROM xqc_ods.web_log_all
       WHERE `day` = {{ ds_nodash }}
         AND `event` = '$pageview'
         AND url LIKE '%xh-mc.xiaoduoai.com/%'
         AND app_id IN ('xd001',
                        'xd023')
         AND shot_name GLOBAL IN
           (SELECT shot_name
            FROM xqc_dim.company)
       GROUP BY shot_name,
                platform),
         employee_snick AS
      (SELECT count(distinct(snick)) AS snick_cnt,
              company_id,
              platform
       FROM ods.xinghuan_employee_snick_all
       WHERE `day` = toYYYYMMDD(yesterday())
       GROUP BY company_id,
                platform),
         task_record_all AS
      (SELECT company_id AS company_id,
              platform,
              count(company_id) AS manual_qc_task_cnt,
              sum(target_num) AS manual_qc_target_dialog_sum,
              sum(mark_num) AS manual_qc_finished_dialog_sum,
              sum(ontime_mark_num) AS manual_qc_ontime_dialog_sum,
              sum(overdue_mark_num) AS manual_qc_overdue_dialog_sum
       FROM xqc_dwd.manual_task_record_all
       WHERE `day` = toYYYYMMDD(yesterday())
       GROUP BY company_id,
                platform),
         manual_task_all AS
      (SELECT company_id,
              platform,
              sum(task_grade = 0) AS manual_qc_basic_dialog_sum,
              sum(task_grade = 1) AS manual_qc_advanced_dialog_sum
       FROM xqc_dwd.manual_task_all
       GROUP BY company_id,
                platform)
    SELECT ads_shop.`day` AS `day`,
           ads_shop.platform AS platform,
           ads_shop.company_id AS company_id,
           customer.name AS company_name,
           customer.shot_name AS company_short_name,
           if(web_log_all.is_active='True', 'True', 'False') AS is_active,
           web_log_all.pv AS pv,
           web_log_all.uv AS uv,
           ads_shop.shop_cnt AS shop_cnt,
           employee_snick.snick_cnt AS snick_cnt,
           ads_shop.snick_uv AS snick_uv,
           ads_shop.cnick_uv AS cnick_uv,
           web_log_all.account_uv AS account_uv,
           ads_shop.subtract_score_sum AS subtract_score_sum,
           ads_shop.add_score_sum AS add_score_sum,
           ads_shop.ai_subtract_score_sum AS ai_subtract_score_sum,
           ads_shop.ai_add_score_sum AS ai_add_score_sum,
           ads_shop.custom_subtract_score_sum AS custom_subtract_score_sum,
           ads_shop.custom_add_score_sum AS custom_add_score_sum,
           ads_shop.manual_subtract_score_sum AS manual_subtract_score_sum,
           ads_shop.manual_add_score_sum AS manual_add_score_sum,
           ads_shop.dialog_cnt AS dialog_cnt,
           ads_shop.tagged_dialog_cnt AS tagged_dialog_cnt,
           ads_shop.ai_tagged_dialog_cnt AS ai_tagged_dialog_cnt,
           ads_shop.custom_tagged_dialog_cnt AS custom_tagged_dialog_cnt,
           ads_shop.manual_tagged_dialog_cnt AS manual_tagged_dialog_cnt,
           ads_shop.subtract_score_dialog_cnt AS subtract_score_dialog_cnt,
           ads_shop.add_score_dialog_cnt AS add_score_dialog_cnt,
           ads_shop.manual_marked_dialog_cnt AS manual_marked_dialog_cnt,
           ads_shop.ai_subtract_score_dialog_cnt AS ai_subtract_score_dialog_cnt,
           ads_shop.ai_add_score_dialog_cnt AS ai_add_score_dialog_cnt,
           ads_shop.custom_subtract_score_dialog_cnt AS custom_subtract_score_dialog_cnt,
           ads_shop.custom_add_score_dialog_cnt AS custom_add_score_dialog_cnt,
           ads_shop.manual_subtract_score_dialog_cnt AS manual_subtract_score_dialog_cnt,
           ads_shop.manual_add_score_dialog_cnt AS manual_add_score_dialog_cnt,
           ads_shop.alert_cnt AS alert_cnt,
           ads_shop.level_1_alert_cnt AS level_1_alert_cnt,
           ads_shop.level_2_alert_cnt AS level_2_alert_cnt,
           ads_shop.level_3_alert_cnt AS level_3_alert_cnt,
           ads_shop.level_1_alert_finished_cnt AS level_1_alert_finished_cnt,
           ads_shop.level_2_alert_finished_cnt AS level_2_alert_finished_cnt,
           ads_shop.level_3_alert_finished_cnt AS level_3_alert_finished_cnt,
           ads_shop.alert_finished_mins AS alert_finished_mins,
           task_record_all.manual_qc_task_cnt AS manual_qc_task_cnt,
           task_record_all.manual_qc_target_dialog_sum AS manual_qc_target_dialog_sum,
           task_record_all.manual_qc_finished_dialog_sum AS manual_qc_finished_dialog_sum,
           task_record_all.manual_qc_ontime_dialog_sum AS manual_qc_ontime_dialog_sum,
           task_record_all.manual_qc_overdue_dialog_sum AS manual_qc_overdue_dialog_sum,
           manual_task_all.manual_qc_basic_dialog_sum AS manual_qc_basic_dialog_sum,
           manual_task_all.manual_qc_advanced_dialog_sum AS manual_qc_advanced_dialog_sum,
           ads_shop.wiped_tag_cnt AS wiped_tag_cnt,
           ads_shop.wiped_ai_tag_cnt AS wiped_ai_tag_cnt,
           ads_shop.wiped_manual_tag_cnt AS wiped_manual_tag_cnt,
           ads_shop.wiped_custom_tag_cnt AS wiped_custom_tag_cnt,
           ads_shop.eval_cnt AS eval_cnt,
           eval_level_t.eval_levels AS eval_levels,
           eval_level_t.eval_level_cnts AS eval_level_cnts,
           qc_norm.qc_norm_cnt AS qc_norm_cnt,
           qc_norm.qc_norm_opened_cnt AS qc_norm_opened_cnt,
           operation_log.qc_norm_edit_cnt AS qc_norm_edit_cnt,
           qc_rule.tag_cnt AS tag_cnt,
           qc_rule.open_alert_tag_cnt AS open_alert_tag_cnt,
           qc_rule.ai_tag_cnt AS ai_tag_cnt,
           qc_rule.ai_tag_opened_cnt AS ai_tag_opened_cnt,
           qc_rule.custom_tag_cnt AS custom_tag_cnt,
           qc_rule.custom_tag_opened_cnt AS custom_tag_opened_cnt,
           qc_rule.manual_tag_cnt AS manual_tag_cnt,
           qc_rule.manual_tag_opened_cnt AS manual_tag_opened_cnt,
           qc_word.qc_word_cnt AS qc_word_cnt,
           qc_word.qc_word_opened_cnt AS qc_word_opened_cnt,
           appeal_task.appeal_task_cnt AS appeal_task_cnt,
           qt_task.qt_task_cnt AS qt_task_cnt,
           mc_case.dialog_case_cnt AS dialog_case_cnt
    FROM ads_shop
    LEFT JOIN customer ON customer._id = ads_shop.company_id
    LEFT JOIN web_log_all ON customer.shot_name = web_log_all.shot_name
    AND ads_shop.platform = web_log_all.platform
    LEFT JOIN employee_snick ON ads_shop.company_id = employee_snick.company_id
    AND ads_shop.platform = employee_snick.platform
    LEFT JOIN task_record_all ON ads_shop.company_id = task_record_all.company_id
    AND ads_shop.platform = task_record_all.platform
    LEFT JOIN manual_task_all ON ads_shop.company_id = manual_task_all.company_id
    AND ads_shop.platform = manual_task_all.platform
    LEFT JOIN eval_level_t ON ads_shop.company_id = eval_level_t.company_id
    AND ads_shop.platform = eval_level_t.platform
    LEFT JOIN qc_norm ON ads_shop.company_id = qc_norm.company_id
    AND ads_shop.platform = qc_norm.platform
    LEFT JOIN operation_log ON ads_shop.company_id = operation_log.company_id
    AND ads_shop.platform = operation_log.platform
    LEFT JOIN qc_rule ON ads_shop.company_id = qc_rule.company_id
    AND ads_shop.platform = qc_rule.platform
    LEFT JOIN qc_word ON ads_shop.company_id = qc_word.company_id
    AND ads_shop.platform = qc_word.platform
    LEFT JOIN appeal_task ON ads_shop.company_id = appeal_task.company_id
    AND ads_shop.platform = appeal_task.platform
    LEFT JOIN qt_task ON ads_shop.company_id = qt_task.company_id
    AND ads_shop.platform = qt_task.platform
    LEFT JOIN mc_case ON ads_shop.company_id = mc_case.company_id
    AND ads_shop.platform = mc_case.platform
    """
)

xqc_ads_company_stat_all = ClickHouseOperator(
    task_id='xqc_ads_company_stat_all',
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    dag=dag,
    sql="""
    INSERT INTO xqc_ads.company_stat_all
    WITH customer AS
  (SELECT *
   FROM ods.xinghuan_company_all
   WHERE `day` = toYYYYMMDD(yesterday()) ),
     shop AS
  (SELECT count(distinct(seller_nick, platform)) AS shop_cnt,
          count(distinct(platform)) AS platform_cnt,
          company_id
   FROM xqc_dim.xqc_shop_all
   WHERE `day` = toYYYYMMDD(yesterday())
   GROUP BY company_id),
     cshop AS
  (SELECT count(company_id) AS snick_cnt,
          company_id
   FROM ods.xinghuan_employee_snick_all
   WHERE `day` = toYYYYMMDD(yesterday())
   GROUP BY company_id),
     shop_view AS
  (SELECT company_id,
          count(DISTINCT shop_id) AS shop_cnt,
          sum(snick_uv) AS snick_uv,
          sum(cnick_uv) AS cnick_uv,
          sum(subtract_score_sum) AS subtract_score_sum,
          sum(add_score_sum) AS add_score_sum,
          sum(ai_subtract_score_sum) AS ai_subtract_score_sum,
          sum(ai_add_score_sum) AS ai_add_score_sum,
          sum(custom_subtract_score_sum) AS custom_subtract_score_sum,
          sum(custom_add_score_sum) AS custom_add_score_sum,
          sum(manual_subtract_score_sum) AS manual_subtract_score_sum,
          sum(manual_add_score_sum) AS manual_add_score_sum,
          sum(dialog_cnt) AS dialog_cnt,
          sum(tagged_dialog_cnt) AS tagged_dialog_cnt,
          sum(ai_tagged_dialog_cnt)AS ai_tagged_dialog_cnt,
          sum(custom_tagged_dialog_cnt)AS custom_tagged_dialog_cnt,
          sum(manual_tagged_dialog_cnt)AS manual_tagged_dialog_cnt,
          sum(subtract_score_dialog_cnt)AS subtract_score_dialog_cnt,
          sum(add_score_dialog_cnt)AS add_score_dialog_cnt,
          sum(manual_marked_dialog_cnt) AS manual_marked_dialog_cnt,
          sum(ai_subtract_score_dialog_cnt) AS ai_subtract_score_dialog_cnt,
          sum(ai_add_score_dialog_cnt) AS ai_add_score_dialog_cnt,
          sum(custom_subtract_score_dialog_cnt) AS custom_subtract_score_dialog_cnt,
          sum(custom_add_score_dialog_cnt) AS custom_add_score_dialog_cnt,
          sum(manual_subtract_score_dialog_cnt) AS manual_subtract_score_dialog_cnt,
          sum(manual_add_score_dialog_cnt) AS manual_add_score_dialog_cnt,
          sum(alert_cnt) AS alert_cnt,
          sum(level_1_alert_cnt) AS level_1_alert_cnt,
          sum(level_2_alert_cnt) AS level_2_alert_cnt,
          sum(level_3_alert_cnt) AS level_3_alert_cnt,
          sum(level_1_alert_finished_cnt) AS level_1_alert_finished_cnt,
          sum(level_2_alert_finished_cnt) AS level_2_alert_finished_cnt,
          sum(level_3_alert_finished_cnt) AS level_3_alert_finished_cnt,
          sum(alert_finished_mins) AS alert_finished_mins,
          sum(wiped_tag_cnt) AS wiped_tag_cnt,
          sum(wiped_ai_tag_cnt) AS wiped_ai_tag_cnt,
          sum(wiped_manual_tag_cnt) AS wiped_manual_tag_cnt,
          sum(wiped_custom_tag_cnt) AS wiped_custom_tag_cnt,
          sum(eval_cnt) AS eval_cnt
   FROM xqc_ads.shop_stat_all
   WHERE `day` = {{ ds_nodash }}
   GROUP BY company_id),
     eval_view AS
  (WITH t AS
     (SELECT company_id,
             eval_level,
             eval_level_cnt
      FROM xqc_ads.shop_stat_all ARRAY
      JOIN eval_levels AS eval_level,
           eval_level_cnts AS eval_level_cnt
      WHERE `day` = toYYYYMMDD(yesterday()) ) SELECT company_id,
                                                     groupArray(cast(eval_level AS String)) AS eval_levels,
                                                     groupArray(cast(eval_level_cnt AS int)) AS eval_level_cnts
   FROM t
   GROUP BY company_id),
     task_record_all AS
  (SELECT company_id AS company_id,
          count(company_id) AS manual_qc_task_cnt,
          sum(target_num) AS manual_qc_target_dialog_sum,
          sum(mark_num) AS manual_qc_finished_dialog_sum,
          sum(ontime_mark_num) AS manual_qc_ontime_dialog_sum,
          sum(overdue_mark_num) AS manual_qc_overdue_dialog_sum
   FROM xqc_dwd.manual_task_record_all
   WHERE `day` = toYYYYMMDD(yesterday())
   GROUP BY company_id),
     manual_task_all AS
  (SELECT company_id,
          sum(task_grade = 0) AS manual_qc_basic_dialog_sum,
          sum(task_grade = 1) AS manual_qc_advanced_dialog_sum
   FROM xqc_dwd.manual_task_all
   GROUP BY company_id),
     xh_qc_task AS
  (SELECT company_id,
          count(company_id) AS qc_norm_cnt,
          sum(status = 1) AS qc_norm_opened_cnt
   FROM ods.xinghuan_qc_norm_all
   WHERE `day` = toYYYYMMDD(yesterday())
   GROUP BY company_id),
     appeal_task_all AS
  (SELECT company_id,
          count(company_id) AS appeal_task_cnt
   FROM xqc_ods.xqc_appeal_task_all
   GROUP BY company_id),
     qt_task_all AS
  (SELECT company_id,
          count(company_id) AS qt_task_cnt
   FROM xqc_ods.xinghuan_mc_qt_task_all
   GROUP BY company_id),
     rule_all AS
  (SELECT company_id,
          sum(rule_category = 1) AS ai_tag_cnt,
          sum(status = 1
              AND rule_category = 1) AS ai_tag_opened_cnt,
          sum(rule_category = 2) AS manual_tag_cnt,
          sum(status = 1
              AND rule_category = 2) AS manual_tag_opened_cnt,
          sum(rule_category = 3) AS custom_tag_cnt,
          sum(status = 1
              AND rule_category = 3) AS custom_tag_opened_cnt
   FROM xqc_dim.qc_rule_all
   WHERE `day` = toYYYYMMDD(yesterday())
   GROUP BY company_id),
     platform_company AS
  (SELECT sum(pv) AS pv,
          sum(uv) AS uv,
          groupArray(distinct(platform)) AS platforms,
          count(distinct(platform)) AS platform_cnt,
          sum(shop_cnt) AS shop_cnt,
          sum(snick_cnt) AS snick_cnt,
          company_id,
          if(sum(is_active='True') > 0, 'True', 'False') AS is_active,
          sum(qc_word_cnt) AS qc_word_cnt,
          sum(qc_word_opened_cnt) AS qc_word_opened_cnt,
          sum(dialog_case_cnt) AS dialog_case_cnt
   FROM xqc_ads.platform_company_stat_all
   WHERE `day` = {{ ds_nodash }}
   GROUP BY company_id)
SELECT {{ ds_nodash }} AS `day`,
                     customer._id AS company_id,
                     customer.name AS company_name,
                     shot_name AS company_short_name,
                     platform_company.is_active AS is_active,
                     platform_company.pv AS pv,
                     platform_company.uv AS uv,
                     platform_company.platforms AS platforms,
                     shop.platform_cnt AS platform_cnt,
                     shop.shop_cnt AS shop_cnt,
                     cshop.snick_cnt AS snick_cnt,
                     shop_view.snick_uv AS snick_uv,
                     shop_view.cnick_uv AS cnick_uv,
                     shop_view.subtract_score_sum AS subtract_score_sum,
                     shop_view.add_score_sum AS add_score_sum,
                     shop_view.ai_subtract_score_sum AS ai_subtract_score_sum,
                     shop_view.ai_add_score_sum AS ai_add_score_sum,
                     shop_view.custom_subtract_score_sum AS custom_subtract_score_sum,
                     shop_view.custom_add_score_sum AS custom_add_score_sum,
                     shop_view.manual_subtract_score_sum AS manual_subtract_score_sum,
                     shop_view.manual_add_score_sum AS manual_add_score_sum,
                     shop_view.dialog_cnt AS dialog_cnt,
                     shop_view.tagged_dialog_cnt AS tagged_dialog_cnt,
                     shop_view.ai_tagged_dialog_cnt AS ai_tagged_dialog_cnt,
                     shop_view.custom_tagged_dialog_cnt AS custom_tagged_dialog_cnt,
                     shop_view.manual_tagged_dialog_cnt AS manual_tagged_dialog_cnt,
                     shop_view.subtract_score_dialog_cnt AS subtract_score_dialog_cnt,
                     shop_view.add_score_dialog_cnt AS add_score_dialog_cnt,
                     shop_view.manual_marked_dialog_cnt AS manual_marked_dialog_cnt,
                     shop_view.ai_subtract_score_dialog_cnt AS ai_subtract_score_dialog_cnt,
                     shop_view.ai_add_score_dialog_cnt AS ai_add_score_dialog_cnt,
                     shop_view.custom_subtract_score_dialog_cnt AS custom_subtract_score_dialog_cnt,
                     shop_view.custom_add_score_dialog_cnt AS custom_add_score_dialog_cnt,
                     shop_view.manual_subtract_score_dialog_cnt AS manual_subtract_score_dialog_cnt,
                     shop_view.manual_add_score_dialog_cnt AS manual_add_score_dialog_cnt,
                     shop_view.alert_cnt AS alert_cnt,
                     shop_view.level_1_alert_cnt AS level_1_alert_cnt,
                     shop_view.level_2_alert_cnt AS level_2_alert_cnt,
                     shop_view.level_3_alert_cnt AS level_3_alert_cnt,
                     shop_view.level_1_alert_finished_cnt AS level_1_alert_finished_cnt,
                     shop_view.level_2_alert_finished_cnt AS level_2_alert_finished_cnt,
                     shop_view.level_3_alert_finished_cnt AS level_3_alert_finished_cnt,
                     shop_view.alert_finished_mins AS alert_finished_mins,
                     task_record_all.manual_qc_task_cnt AS manual_qc_task_cnt,
                     task_record_all.manual_qc_target_dialog_sum AS manual_qc_target_dialog_sum,
                     task_record_all.manual_qc_finished_dialog_sum AS manual_qc_finished_dialog_sum,
                     task_record_all.manual_qc_ontime_dialog_sum AS manual_qc_ontime_dialog_sum,
                     task_record_all.manual_qc_overdue_dialog_sum AS manual_qc_overdue_dialog_sum,
                     manual_task_all.manual_qc_basic_dialog_sum AS manual_qc_basic_dialog_sum,
                     manual_task_all.manual_qc_advanced_dialog_sum AS manual_qc_advanced_dialog_sum,
                     shop_view.wiped_tag_cnt AS wiped_tag_cnt,
                     shop_view.wiped_ai_tag_cnt AS wiped_ai_tag_cnt,
                     shop_view.wiped_manual_tag_cnt AS wiped_manual_tag_cnt,
                     shop_view.wiped_custom_tag_cnt AS wiped_custom_tag_cnt,
                     shop_view.eval_cnt AS eval_cnt,
                     eval_view.eval_levels AS eval_levels,
                     eval_view.eval_level_cnts AS eval_level_cnts,
                     xh_qc_task.qc_norm_cnt AS qc_norm_cnt,
                     xh_qc_task.qc_norm_opened_cnt AS qc_norm_opened_cnt,
                     rule_all.ai_tag_cnt AS ai_tag_cnt,
                     rule_all.ai_tag_opened_cnt AS ai_tag_opened_cnt,
                     rule_all.custom_tag_cnt AS custom_tag_cnt,
                     rule_all.custom_tag_opened_cnt AS custom_tag_opened_cnt,
                     rule_all.manual_tag_cnt AS manual_tag_cnt,
                     rule_all.manual_tag_opened_cnt AS manual_tag_opened_cnt,
                     platform_company.qc_word_cnt AS qc_word_cnt,
                     platform_company.qc_word_opened_cnt AS qc_word_opened_cnt,
                     appeal_task_all.appeal_task_cnt AS appeal_task_cnt,
                     qt_task_all.qt_task_cnt AS qt_task_cnt,
                     platform_company.dialog_case_cnt AS dialog_case_cnt
FROM customer
LEFT JOIN shop ON shop.company_id = customer._id
LEFT JOIN cshop ON cshop.company_id = customer._id
LEFT JOIN shop_view ON shop_view.company_id = customer._id
LEFT JOIN eval_view ON eval_view.company_id = customer._id
LEFT JOIN task_record_all ON task_record_all.company_id = customer._id
LEFT JOIN manual_task_all ON manual_task_all.company_id = customer._id
LEFT JOIN xh_qc_task ON xh_qc_task.company_id = customer._id
LEFT JOIN appeal_task_all ON appeal_task_all.company_id = customer._id
LEFT JOIN qt_task_all ON qt_task_all.company_id = customer._id
LEFT JOIN rule_all ON rule_all.company_id = customer._id
LEFT JOIN platform_company ON platform_company.company_id = customer._id
    """
)

truncate_ch_tmp_table >> [
    dwd_manual_task_record_all,
    ods_dwd_manual_task_all,
    dwd_wiped_tag_all
] >> clean_open_stat_partition

clean_open_stat_partition >> [
    xqc_dws_dialog_eval_stat_all,
    xqc_dws_alert_tag_stat_all,
    xqc_dws_qc_norm_stat_all
] >> xqc_ads_shop_stat_all >> xqc_ads_platform_company_stat_all
xqc_ads_platform_company_stat_all >> xqc_ads_company_stat_all
