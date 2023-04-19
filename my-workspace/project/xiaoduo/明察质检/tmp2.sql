import logging as log
import time
from collections.abc import Iterable
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator

VOC_COMPANY_SHOP_LIST = [
    {
        "company_id": "63fc50f0a06a5ecd9a249ac9",
        "shop_id": "61616faa112fa5000dcc7fba",
        "platform": "jd",
    },
]

VOC_COMPANY_IDS = [x["company_id"] for x in VOC_COMPANY_SHOP_LIST]
VOC_SHOP_IDS = [x["shop_id"] for x in VOC_COMPANY_SHOP_LIST]
VOC_PLATFORMS = [x["platform"] for x in VOC_COMPANY_SHOP_LIST]

MONGO_CONN_ID = "xdqc_mongo_tb"
MONGO_DIM_DB = "xqc"

CH_CONN_ID = "clickhouse_zjk_008"
CH_CLUSTER = "cluster_3s_2r"
CH_HOOK = None
CH_TMP_DATA_TTL_DAYS = 7

args = {
    'owner': 'chengcheng',
    'start_date': datetime(2023, 3, 1),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}


def get_ch_hook() -> ClickHouseHook:
    # singleton pattern for generating ClickHouse connection
    global CH_HOOK
    if CH_HOOK is None:
        CH_HOOK = ClickHouseHook(CH_CONN_ID)
    return CH_HOOK


def ch_execute(sql: str, sleep_sec: int = 1):
    ch_hook = get_ch_hook()

    log.info(sql)
    res = ch_hook.execute(sql)
    time.sleep(sleep_sec)

    return res


def ch_table_truncate(
    clear_table, ch_cluster=CH_CLUSTER
):
    ch_hook = get_ch_hook()
    ch_drop_sql = f"""
        TRUNCATE TABLE {clear_table} ON CLUSTER {ch_cluster}
    """

    log.info(ch_drop_sql)
    log.info(ch_hook.execute(ch_drop_sql))
    time.sleep(3)


def ch_table_drop_partition(
    clear_table, partition_expr, ch_cluster=CH_CLUSTER
):
    ch_hook = get_ch_hook()
    ch_drop_sql = f"""
        ALTER TABLE {clear_table} ON CLUSTER {ch_cluster}
        DROP PARTITION {partition_expr}
    """

    log.info(ch_drop_sql)
    log.info(ch_hook.execute(ch_drop_sql))
    time.sleep(3)


def ch_drop_partition_and_stat_func(
    clear_table, partition_expr, stat_sqls
):
    ch_table_drop_partition(clear_table, partition_expr)

    ch_hook = get_ch_hook()
    if isinstance(stat_sqls, str):
        log.info(stat_sqls)
        log.info(ch_hook.execute(stat_sqls))
    elif isinstance(stat_sqls, Iterable):
        for sql in stat_sqls:
            log.info(sql)
            log.info(ch_hook.execute(sql))
            time.sleep(1)


def mongo_full_update_to_ch_atomic_operator(
    mongo_conn_id, mongo_db, mongo_collection,
    ch_conn_id, ch_cluster, ch_table, ch_local_table,
    doc_mapper=None
):
    log.info(
        mongo_conn_id, mongo_db, mongo_collection,
        ch_conn_id, ch_cluster, ch_table, ch_local_table,
        doc_mapper
    )

    ch_table_truncate(ch_local_table, ch_cluster)

    # full update
    MongoToClickHouseOperator(
        task_id=f"extract_{mongo_collection}_to_{ch_table}",
        mongo_conn_id=mongo_conn_id,
        clickhouse_conn_id=ch_conn_id,
        mongo_db=mongo_db,
        mongo_collection=mongo_collection,
        destination_ch_table=ch_table,
        aggregate_query=None,
        flatten_map=doc_mapper
    ).execute({})


def dict_time_mapper(doc: dict) -> list:
    res_list = []
    if "create_time" in doc and isinstance(doc.get("create_time"), datetime):
        create_time: datetime = doc.get("create_time")
        doc["create_time"] = int(create_time.timestamp() * 1000)

    if "update_time" in doc and isinstance(doc.get("update_time"), datetime):
        update_time: datetime = doc.get("update_time")
        doc["update_time"] = int(update_time.timestamp() * 1000)

    res_list.append(doc)
    return res_list


with DAG(
    dag_id='voc_ods_etl_jd',
    default_args=args,
    schedule_interval="0 5 * * *",
    max_active_runs=1,
    concurrency=3,
    catchup=True,
    tags=['T+1', 'VOC'],
) as dag:
    dim_voc_question_b_group_etl = PythonOperator(
        task_id="dim_voc_question_b_group_etl",
        python_callable=mongo_full_update_to_ch_atomic_operator,
        op_kwargs={
            "mongo_conn_id": MONGO_CONN_ID,
            "mongo_db": MONGO_DIM_DB,
            "mongo_collection": "question_group",
            "ch_conn_id": CH_CONN_ID,
            "ch_cluster": CH_CLUSTER,
            "ch_table": "dim.voc_question_b_group_all",
            "ch_local_table": "dim.voc_question_b_group_local",
            "doc_mapper": dict_time_mapper
        },
        dag=dag
    )

    dim_voc_question_b_etl = PythonOperator(
        task_id="dim_voc_question_b_etl",
        python_callable=mongo_full_update_to_ch_atomic_operator,
        op_kwargs={
            "mongo_conn_id": MONGO_CONN_ID,
            "mongo_db": MONGO_DIM_DB,
            "mongo_collection": "question",
            "ch_conn_id": CH_CONN_ID,
            "ch_cluster": CH_CLUSTER,
            "ch_table": "dim.voc_question_b_all",
            "ch_local_table": "dim.voc_question_b_local",
            "doc_mapper": dict_time_mapper
        },
        dag=dag
    )


    def dim_voc_question_b_group_detail_etl_func(ds_nodash):
        clear_table = "dim.voc_question_b_group_detail_local"
        sink_table = "dim.voc_question_b_group_detail_all"

        ch_etl_sql = f"""
            INSERT INTO {sink_table}
            SELECT
                company_id,
                group_id,
                group_name,
                group_level,
                parent_group_ids[1] AS parent_group_id,
                parent_group_names[1] AS parent_group_name,
                parent_group_ids[-1] AS first_group_id,
                parent_group_names[-1] AS first_group_name,
                parent_group_ids[-2] AS second_group_id,
                parent_group_names[-2] AS second_group_name,
                parent_group_ids[-3] AS third_group_id,
                parent_group_names[-3] AS third_group_name,
                parent_group_ids[-4] AS fourth_group_id,
                parent_group_names[-4] AS fourth_group_name,
                create_time,
                update_time
            FROM (
                SELECT
                    child.company_id AS company_id,
                    child.group_id AS group_id,
                    child.group_name AS group_name,
                    child.group_level AS group_level,
                    parent.parent_id AS next_parent_group_id,
                    IF(parent.group_id!='', arrayPushBack(parent_group_ids, parent.group_id), parent_group_ids) AS parent_group_ids,
                    IF(parent.group_id!='', arrayPushBack(parent_group_names, parent.group_name), parent_group_names) AS parent_group_names,
                    child.create_time AS create_time,
                    child.update_time AS update_time
                FROM (
                    SELECT
                        child.company_id AS company_id,
                        child.group_id AS group_id,
                        child.group_name AS group_name,
                        child.group_level AS group_level,
                        parent.parent_id AS next_parent_group_id,
                        IF(parent.group_id!='', arrayPushBack(parent_group_ids, parent.group_id), parent_group_ids) AS parent_group_ids,
                        IF(parent.group_id!='', arrayPushBack(parent_group_names, parent.group_name), parent_group_names) AS parent_group_names,
                        child.create_time AS create_time,
                        child.update_time AS update_time
                    FROM (
                        SELECT
                            child.company_id AS company_id,
                            child.group_id AS group_id,
                            child.group_name AS group_name,
                            child.group_level AS group_level,
                            parent.parent_id AS next_parent_group_id,
                            IF(parent.group_id!='', arrayPushBack(parent_group_ids, parent.group_id), parent_group_ids) AS parent_group_ids,
                            IF(parent.group_id!='', arrayPushBack(parent_group_names, parent.group_name), parent_group_names) AS parent_group_names,
                            child.create_time AS create_time,
                            child.update_time AS update_time
                        FROM (
                            SELECT
                                company_id,
                                _id AS group_id,
                                name AS group_name,
                                level AS group_level,
                                parent_id AS next_parent_group_id,
                                [] AS parent_group_ids,
                                [] AS parent_group_names,
                                create_time,
                                update_time
                            FROM dim.voc_question_b_group_all
                        ) AS child
                        GLOBAL LEFT JOIN (
                            SELECT
                                _id AS group_id,
                                name AS group_name,
                                parent_id
                            FROM dim.voc_question_b_group_all
                        ) AS parent
                        ON child.next_parent_group_id = parent.group_id
                    ) AS child
                    GLOBAL LEFT JOIN (
                        SELECT
                            _id AS group_id,
                            name AS group_name,
                            parent_id
                        FROM dim.voc_question_b_group_all
                    ) AS parent
                    ON child.next_parent_group_id = parent.group_id
                ) AS child
                GLOBAL LEFT JOIN (
                    SELECT
                        _id AS group_id,
                        name AS group_name,
                        parent_id
                    FROM dim.voc_question_b_group_all
                ) AS parent
                ON child.next_parent_group_id = parent.group_id
            )
        """
        ch_table_truncate(clear_table)
        ch_execute(ch_etl_sql)


    dim_voc_question_b_group_detail_etl = PythonOperator(
        task_id="dim_voc_question_b_group_detail_etl",
        python_callable=dim_voc_question_b_group_detail_etl_func,
        op_kwargs={
            "ds_nodash": "{{ds_nodash}}"
        },
        dag=dag
    )


    def dim_voc_question_b_detail_etl_func(ds_nodash):
        clear_table = "dim.voc_question_b_detail_local"
        sink_table = "dim.voc_question_b_detail_all"

        ch_etl_sql = f"""
            INSERT INTO {sink_table}
            SELECT
                company_id,
                question_b_qid,
                question_b_name,
                group_id,
                group_name,
                group_level,
                parent_group_id,
                parent_group_name,
                first_group_id,
                first_group_name,
                second_group_id,
                second_group_name,
                third_group_id,
                third_group_name,
                fourth_group_id,
                fourth_group_name
                create_time,
                update_time
            FROM (
                SELECT
                    company_id,
                    question_b_qid,
                    question_b_name,
                    group_id,
                    create_time,
                    update_time
                FROM (
                    SELECT
                        company_id,
                        name AS question_b_name,
                        group_id, 
                        create_time,
                        update_time
                    FROM dim.voc_question_b_all
                    WHERE company_id IN {VOC_COMPANY_IDS}
                ) AS voc_question_info
                LEFT JOIN (
                    -- 获取企业店铺行业场景
                    SELECT
                        company_id,
                        shop_id,
                        category_id,
                        subcategory_id,
                        question_b_qid,
                        question_b_name
                    FROM (
                        -- 获取企业店铺行业场景一级分组
                        SELECT
                            company_id,
                            shop_id,
                            category_id,
                            subcategory_id
                        FROM (
                            -- 获取企业店铺品类
                            SELECT
                                company_id,
                                shop_id,
                                category_id
                            FROM (
                                SELECT
                                    _id AS shop_id,
                                    category_id
                                FROM dim.xdre_shop_all
                                WHERE _id IN {VOC_SHOP_IDS}
                            ) AS shop_subcategory_info
                            GLOBAL INNER JOIN (
                                SELECT
                                    company_id,
                                    shop_id
                                FROM numbers(1)
                                ARRAY JOIN
                                    {VOC_COMPANY_IDS} AS company_id,
                                    {VOC_SHOP_IDS} AS shop_id
                            ) AS voc_shop_info
                            USING(shop_id)
                        ) AS company_shop_subcategory_info
                        GLOBAL INNER JOIN (
                            SELECT
                                category_id,
                                subcategory_id
                            FROM dim.category_subcategory_all
                        ) AS cate_map_info
                        USING(category_id)
                    ) AS company_shop_subcategory_info
                    INNER JOIN (
                        SELECT
                            qid AS question_b_qid,
                            question AS question_b_name,
                            subcategory_id
                        FROM dim.question_b_v2_all
                    ) AS question_b_info
                    USING(subcategory_id)
                ) AS robot_question_info
                USING(company_id, question_b_name)
            )
            LEFT JOIN (
                SELECT
                    company_id,
                    group_id,
                    group_name,
                    group_level,
                    parent_group_id,
                    parent_group_name,
                    first_group_id,
                    first_group_name,
                    second_group_id,
                    second_group_name,
                    third_group_id,
                    third_group_name,
                    fourth_group_id,
                    fourth_group_name
                FROM dim.voc_question_b_group_detail_all
                WHERE company_id IN {VOC_COMPANY_IDS}
            ) AS company_group_info
            USING(company_id, group_id)

        """

        ch_table_truncate(clear_table)
        ch_execute(ch_etl_sql)


    dim_voc_question_b_detail_etl = PythonOperator(
        task_id="dim_voc_question_b_detail_etl",
        python_callable=dim_voc_question_b_detail_etl_func,
        op_kwargs={
            "ds_nodash": "{{ds_nodash}}"
        },
        dag=dag
    )


    def dwd_voc_cnick_all_etl_func(ds_nodash, yesterday_ds_nodash):
        ds_dt = datetime.strptime(str(ds_nodash), "%Y%m%d")
        expired_dt = ds_dt - timedelta(days=CH_TMP_DATA_TTL_DAYS + 1)

        ds_nodash = int(ds_nodash)
        expired_ds_nodash = int(expired_dt.strftime("%Y%m%d"))

        # VOC咨询买家名单表
        log.info("VOC咨询买家名单表")

        dwd_voc_cnick_etl_sink_local_table = "dwd.voc_cnick_list_local"
        dwd_voc_cnick_etl_sink_table = "dwd.voc_cnick_list_all"
        dwd_voc_cnick_etl_sql = f"""
            INSERT INTO {dwd_voc_cnick_etl_sink_table}
            -- 查询最大cnick_id, 实现自增ID, 离线构建cnick one_id
            WITH (
                SELECT max(cnick_id) + 1
                FROM {dwd_voc_cnick_etl_sink_table}
                WHERE day = {yesterday_ds_nodash}
            ) AS max_cnick_id
            SELECT
                day,
                platform,
                cnick,
                real_buyer_nick,
                max_cnick_id + rowNumberInAllBlocks() AS cnick_id
            FROM (
                SELECT DISTINCT
                    day,
                    platform,
                    replaceOne(cnick,'cnjd','') AS cnick,
                    '' AS real_buyer_nick
                FROM ods.xdrs_logs_all
                WHERE day = {ds_nodash}
                AND shop_id IN {VOC_SHOP_IDS}
            ) AS today_cnick_list
            WHERE (platform, cnick) NOT IN (
                -- 剔除已有子账号记录
                SELECT platform, cnick
                FROM {dwd_voc_cnick_etl_sink_table}
                WHERE day = {yesterday_ds_nodash}
            ) AS yesterday_cnick_list
        """

        ch_table_drop_partition(dwd_voc_cnick_etl_sink_local_table, expired_ds_nodash)
        ch_drop_partition_and_stat_func(
            dwd_voc_cnick_etl_sink_local_table, ds_nodash, dwd_voc_cnick_etl_sql
        )


    dwd_voc_cnick_all_etl = PythonOperator(
        task_id="dwd_voc_cnick_all_etl",
        python_callable=dwd_voc_cnick_all_etl_func,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "yesterday_ds_nodash": "{{ yesterday_ds_nodash }}"
        },
        dag=dag
    )


    def dwd_voc_buyer_latest_order_etl_func(ds_nodash, yesterday_ds_nodash):
        ds_dt = datetime.strptime(str(ds_nodash), "%Y%m%d")
        expired_dt = ds_dt - timedelta(days=CH_TMP_DATA_TTL_DAYS + 1)

        ds_nodash = int(ds_nodash)
        expired_ds_nodash = int(expired_dt.strftime("%Y%m%d"))

        # VOC买家最新订单记录
        log.info("VOC买家最新订单记录")

        dwd_voc_buyer_latest_order_etl_sink_local_table = "dwd.voc_buyer_latest_order_local"
        dwd_voc_buyer_latest_order_etl_sink_table = "dwd.voc_buyer_latest_order_all"
        dwd_voc_cnick_etl_sql_1 = f"""
            INSERT INTO {dwd_voc_buyer_latest_order_etl_sink_table}
            -- 查询每个买家当天创建的订单
            SELECT
                day,
                platform,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                order_id,
                arraySort(groupArray(timestamp)) AS order_status_timestamps,
                arraySort((x, y)->y, groupArray(status), groupArray(timestamp)) AS order_statuses
            FROM (
                SELECT
                    day, platform, shop_id, buyer_nick, real_buyer_nick,
                    order_id, timestamp, status
                FROM (
                    SELECT DISTINCT
                        toUInt32(day) AS day,
                        shop_id,
                        buyer_nick,
                        '' AS real_buyer_nick,
                        order_id,
                        toUInt64(time) AS timestamp,
                        status
                    FROM ods.order_event_all
                    WHERE day = {ds_nodash}
                    AND shop_id IN {VOC_SHOP_IDS}
                    -- 筛选当天每个买家的最新创建的订单
                    AND order_id IN (
                        SELECT latest_order_id
                        FROM (
                            SELECT
                                buyer_nick,
                                arrayReverseSort(
                                    (x,y)->y, order_ids, times
                                )[1] AS latest_order_id,
                                groupArray(order_id) AS order_ids,
                                groupArray(time) AS times
                            FROM ods.order_event_all
                            WHERE day = {ds_nodash}
                            AND shop_id IN {VOC_SHOP_IDS}
                            AND status = 'created'
                            GROUP BY buyer_nick
                        )
                    )
                ) AS order_create_info
                GLOBAL LEFT JOIN (
                    SELECT
                        company_id,
                        shop_id,
                        platform
                    FROM numbers(1)
                    ARRAY JOIN
                        {VOC_COMPANY_IDS} AS company_id,
                        {VOC_SHOP_IDS} AS shop_id,
                        {VOC_PLATFORMS} AS platform
                ) AS voc_shop_info
                USING(shop_id)
            ) AS order_create_info
            GROUP BY day,
                platform,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                order_id
        """
        dwd_voc_cnick_etl_sql_2 = f"""
            INSERT INTO {dwd_voc_buyer_latest_order_etl_sink_table}
            -- 查询每个买家过去创建的订单, 并获取其当天的更新记录
            SELECT
                day,
                platform,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                order_id,
                arrayConcat(past_order_info.order_status_timestamps, past_order_update_info.order_status_timestamps) AS order_status_timestamps,
                arrayConcat(past_order_info.order_statuses, past_order_update_info.order_statuses) AS order_statuses
            FROM (
                -- 过去创建的订单记录, 剔除当天有下单的买家
                SELECT
                    *
                FROM {dwd_voc_buyer_latest_order_etl_sink_table}
                WHERE day = {yesterday_ds_nodash}
                AND shop_id IN {VOC_SHOP_IDS}
                -- 剔除当天下过单的买家
                AND buyer_nick NOT IN (
                    SELECT
                        buyer_nick
                    FROM ods.order_event_all
                    WHERE day = {ds_nodash}
                    AND shop_id IN {VOC_SHOP_IDS}
                    AND status = 'created'
                )
            ) AS past_order_info
            LEFT JOIN (
                -- 当天产生但在过去创建的订单记录, 用于更新过去创建的订单记录状态字段, 剔除当天有下单的买家
                SELECT
                    day,
                    platform,
                    shop_id,
                    buyer_nick,
                    real_buyer_nick,
                    order_id,
                    arraySort(groupArray(timestamp)) AS order_status_timestamps,
                    arraySort((x, y)->y, groupArray(status), groupArray(timestamp)) AS order_statuses
                FROM (
                    SELECT
                        day, platform, shop_id, buyer_nick, real_buyer_nick,
                        order_id, timestamp, status
                    FROM (
                        SELECT DISTINCT
                            toUInt32(day) AS day,
                            shop_id,
                            buyer_nick,
                            '' AS real_buyer_nick,
                            order_id,
                            toUInt64(time) AS timestamp,
                            status
                        FROM ods.order_event_all
                        WHERE day = {ds_nodash}
                        AND shop_id IN {VOC_SHOP_IDS}
                        -- 剔除当天下过单的买家
                        AND buyer_nick NOT IN (
                            SELECT 
                                buyer_nick
                            FROM ods.order_event_all
                            WHERE day = {ds_nodash}
                            AND shop_id IN {VOC_SHOP_IDS}
                            AND status = 'created'
                        )
                    ) AS order_update_info
                    GLOBAL LEFT JOIN (
                        SELECT
                            company_id,
                            shop_id,
                            platform
                        FROM numbers(1)
                        ARRAY JOIN
                            {VOC_COMPANY_IDS} AS company_id,
                            {VOC_SHOP_IDS} AS shop_id,
                            {VOC_PLATFORMS} AS platform
                    ) AS voc_shop_info
                    USING(shop_id)
                ) AS order_update_info
                GROUP BY day,
                    platform,
                    shop_id,
                    buyer_nick,
                    real_buyer_nick,
                    order_id
            ) AS past_order_update_info
            USING(
                day,
                platform,
                shop_id,
                buyer_nick,
                real_buyer_nick,
                order_id
            )
        """

        ch_table_drop_partition(dwd_voc_buyer_latest_order_etl_sink_local_table, expired_ds_nodash)
        ch_table_drop_partition(dwd_voc_buyer_latest_order_etl_sink_local_table, ds_nodash)
        ch_execute(dwd_voc_cnick_etl_sql_1, 10)
        ch_execute(dwd_voc_cnick_etl_sql_2, 10)


    dwd_voc_buyer_latest_order_etl = PythonOperator(
        task_id="dwd_voc_buyer_latest_order_etl",
        python_callable=dwd_voc_buyer_latest_order_etl_func,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "yesterday_ds_nodash": "{{ yesterday_ds_nodash }}"
        },
        dag=dag
    )


    def dwd_voc_chat_log_detail_etl_func(ds_nodash, yesterday_ds_nodash):
        ds_dt = datetime.strptime(str(ds_nodash), "%Y%m%d")
        ds_nodash = int(ds_nodash)

        # VOC聊天订单状态表
        log.info("VOC聊天订单状态表")
        dwd_voc_chat_log_detail_etl_sink_local_table = "dwd.voc_chat_log_detail_local"
        dwd_voc_chat_log_detail_etl_sink_table = "dwd.voc_chat_log_detail_all"
        dwd_voc_chat_log_detail_etl_sql = f"""
            INSERT INTO {dwd_voc_chat_log_detail_etl_sink_table}
            SELECT
                day,
                platform,
                shop_id,
                snick,
                cnick,
                cnick_id,
                real_buyer_nick,
                msg_timestamp,
                msg_id,
                msg,
                act,
                send_msg_from,
                question_b_qid,
                plat_goods_id,
                IF(recent_order_status!='', latest_order_info.order_id, '') AS recent_order_id,
                arrayFilter(
                    (x)-> x<=msg_timestamp,
                    latest_order_info.order_status_timestamps
                )[-1] AS recent_order_status_timestamp,
                arrayFilter(
                    (x,y)-> y<=msg_timestamp,
                    latest_order_info.order_statuses,
                    latest_order_info.order_status_timestamps
                )[-1] AS recent_order_status,
                dialog_qa_cnt AS dialog_qa_sum
            FROM (
                SELECT
                    day,
                    platform,
                    shop_id,
                    snick,
                    cnick,
                    cnick_id,
                    real_buyer_nick,
                    msg_timestamp,
                    msg_id,
                    msg,
                    act,
                    send_msg_from,
                    question_b_qid,
                    plat_goods_id,
                    dialog_detail_info.dialog_qa_cnt
                FROM (
                    -- stage_1, 获取当天的聊天消息记录
                    SELECT
                        toUInt32(day) AS day,
                        platform,
                        shop_id,
                        replaceOne(snick,'cnjd','') AS snick,
                        replaceOne(cnick,'cnjd','') AS cnick,
                        '' AS real_buyer_nick,
                        toUInt64(msg_time) AS msg_timestamp,
                        msg_id,
                        msg,
                        act,
                        send_msg_from,
                        question_b_qid,
                        plat_goods_id
                    FROM ods.xdrs_logs_all
                    WHERE day = {ds_nodash}
                    AND shop_id IN {VOC_SHOP_IDS}
                    AND act IN ['send_msg', 'recv_msg']
                ) AS xdrs_logs
                LEFT JOIN (
                    SELECT
                        day,
                        platform,
                        shop_id,
                        snick,
                        cnick,
                        cnick_info.cnick_id,
                        real_buyer_nick,
                        dialog_qa_cnt
                    FROM (
                        -- stage_2, 使用当天的聊天消息计算会话轮次, 按照会话聚合, 统计会话轮次
                        SELECT
                            day,
                            platform,
                            shop_id,
                            snick,
                            cnick,
                            real_buyer_nick,
                            arraySort(groupArray(msg_milli_timestamp)) AS msg_milli_timestamps,
                            arraySort((x, y)->y, groupArray(act), groupArray(msg_milli_timestamp)) AS msg_acts,
            
                            -- 切分会话生成QA切分标记, PS: 可能存在单个Q, 单个A, 单个QA, 多个QA四种情况, 此切分方法只能切分多QA的情况
                            arrayMap(
                                (x, y)->(if(x = 'send_msg' AND msg_acts[y-1] = 'recv_msg', 1, 0)),
                                msg_acts,
                                arrayEnumerate(msg_acts)
                            ) AS _qa_split_tags,
                            -- QA数量
                            arraySum(_qa_split_tags) AS dialog_qa_cnt
                        FROM (
                            SELECT
                                toUInt32(day) AS day,
                                platform,
                                shop_id,
                                replaceOne(snick,'cnjd','') AS snick,
                                replaceOne(cnick,'cnjd','') AS cnick,
                                '' AS real_buyer_nick,
                                toUInt64(toFloat64(toDateTime64(create_time, 3))*1000) AS msg_milli_timestamp,
                                act
                            FROM ods.xdrs_logs_all
                            WHERE day = {ds_nodash}
                            AND shop_id IN {VOC_SHOP_IDS}
                            AND act IN ['send_msg', 'recv_msg']
                        ) AS xdrs_logs
                        GROUP BY day,
                            platform,
                            shop_id,
                            snick,
                            cnick,
                            real_buyer_nick
                    ) AS dialog_info
                    LEFT JOIN (
                        SELECT
                            cnick,
                            cnick_id
                        FROM dwd.voc_cnick_list_all
                        WHERE day = {ds_nodash}
                        AND (platform, cnick) IN (
                            SELECT DISTINCT
                                platform,
                                replaceOne(cnick,'cnjd','') AS cnick
                            FROM ods.xdrs_logs_all
                            WHERE day = {ds_nodash}
                            AND shop_id IN {VOC_SHOP_IDS}
                            AND act IN ['send_msg', 'recv_msg']
                        )
                    ) AS cnick_info
                    USING(cnick)
                ) AS dialog_detail_info
                USING(day, platform, shop_id, snick, cnick, real_buyer_nick)
            ) AS xdrs_dialog_info
            LEFT JOIN (
                -- stage_4, 关联买家最新订单表, 获取订单状态
                SELECT
                    day,
                    buyer_nick AS cnick,
                    order_id,
                    order_status_timestamps,
                    order_statuses
                FROM dwd.voc_buyer_latest_order_all
                WHERE day = {ds_nodash}
                AND shop_id IN {VOC_SHOP_IDS}
            ) AS latest_order_info
            USING(day, cnick)
        """

        ch_table_drop_partition(dwd_voc_chat_log_detail_etl_sink_local_table, ds_nodash)
        ch_execute(dwd_voc_chat_log_detail_etl_sql, 10)


    dwd_voc_chat_log_detail_etl = PythonOperator(
        task_id="dwd_voc_chat_log_detail_etl",
        python_callable=dwd_voc_chat_log_detail_etl_func,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "yesterday_ds_nodash": "{{ yesterday_ds_nodash }}"
        },
        dag=dag
    )


    def dws_voc_goods_question_stat_stat_func(ds_nodash, yesterday_ds_nodash):
        ds_dt = datetime.strptime(str(ds_nodash), "%Y%m%d")
        ds_nodash = int(ds_nodash)

        # VOC商品问题表
        log.info("VOC商品问题表")
        dws_voc_goods_question_stat_stat_sink_local_table = "dws.voc_goods_question_stat_local"
        dws_voc_goods_question_stat_stat_sink_table = "dws.voc_goods_question_stat_all"
        dws_voc_goods_question_stat_stat_sql = f"""
            INSERT INTO {dws_voc_goods_question_stat_stat_sink_table}
            SELECT
                day,
                platform,
                shop_id,
                snick,
                question_id,
                dialog_qa_stage,
                dialog_goods_id,
                recent_order_id,
                recent_order_status,
                recent_order_status_timestamp,
                groupBitmapState(cnick_id) AS cnick_id_bitmap,
                bitmapCardinality(cnick_id_bitmap) AS dialog_sum
            FROM (
                SELECT
                    day,
                    platform,
                    shop_id,
                    snick,
                    cnick_id,
                    question_b_qid AS question_id,
                    CASE
                        WHEN dialog_qa_sum=0 THEN 0
                        WHEN dialog_qa_sum>0 AND dialog_qa_sum<=3 THEN 1
                        WHEN dialog_qa_sum>3 AND dialog_qa_sum<=10 THEN 2
                        ELSE 3
                    END AS dialog_qa_stage,
                    plat_goods_id AS dialog_goods_id,
                    recent_order_id,
                    recent_order_status,
                    recent_order_status_timestamp
                FROM dwd.voc_chat_log_detail_all
                WHERE day = {ds_nodash}
                AND shop_id IN {VOC_SHOP_IDS}
            )
            GROUP BY day,
                platform,
                shop_id,
                snick,
                question_id,
                dialog_qa_stage,
                dialog_goods_id,
                recent_order_id,
                recent_order_status,
                recent_order_status_timestamp

        """

        ch_table_drop_partition(dws_voc_goods_question_stat_stat_sink_local_table, ds_nodash)
        ch_execute(dws_voc_goods_question_stat_stat_sql, 10)


    dws_voc_goods_question_stat_stat = PythonOperator(
        task_id="dws_voc_goods_question_stat_stat",
        python_callable=dws_voc_goods_question_stat_stat_func,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}",
            "yesterday_ds_nodash": "{{ yesterday_ds_nodash }}"
        },
        dag=dag
    )

    [
        dim_voc_question_b_group_etl, dim_voc_question_b_etl
    ] >> dim_voc_question_b_group_detail_etl >> dim_voc_question_b_detail_etl

    dwd_voc_cnick_all_etl >> dwd_voc_buyer_latest_order_etl >> dwd_voc_chat_log_detail_etl >> dws_voc_goods_question_stat_stat
