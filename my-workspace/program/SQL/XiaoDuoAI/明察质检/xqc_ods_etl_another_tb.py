from copy import deepcopy
from datetime import datetime, timedelta
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook

# MONGO_CONN_ID = "xdqc_mongo_mini_test"
# CLICKHOUSE_CONN_ID = 'clickhouse_mini_test'
from bson import ObjectId

MONGO_CONN_ID = "xdqc_offline"
CLICKHOUSE_CONN_ID = 'clickhouse_zjk_008'

args = {
    'owner': 'chenhao03',
    'start_date': datetime(2022, 9, 22),
    'email': ['chenhao03@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG(
    dag_id='xqc_ods_etl_another_tb',
    schedule_interval="30 3 * * *",
    max_active_runs=1,
    default_args=args
)

XH_MONGO_DB = "xinghuan-mc"
XDQC_MONGO_DB = "xdqc"
XQC_MONGO_DB = "xqc"
QC_WIPE_RECORD_COLLECTION = "wipe_record"
TASK_RECORD_COLLECTION = "task_record"


def get_int_day_from_str(date):
    return int(str(date).replace("-", "", -1))


def xqc_task_record_map(data):
    if not data:
        return []
    if "day" in data.keys():
        data.update({"day": get_int_day_from_str(data.get("day"))})
    return [data]


def truncate_ch_tmp_table_func(ds_nodash):
    ch_hook = ClickHouseHook(CLICKHOUSE_CONN_ID)
    ch_hook.execute(f"TRUNCATE TABLE xqc_ods.manual_task_record_local ON CLUSTER cluster_3s_2r")
    ch_hook.execute(f"TRUNCATE TABLE xqc_ods.xinghuan_mc_case_detail_local ON CLUSTER cluster_3s_2r")
    ch_hook.execute(f"TRUNCATE TABLE xqc_ods.xqc_appeal_task_local ON CLUSTER cluster_3s_2r")
    ch_hook.execute(f"TRUNCATE TABLE xqc_ods.xinghuan_mc_qt_task_local ON CLUSTER cluster_3s_2r")
    ch_hook.execute(f"TRUNCATE TABLE xqc_ods.wiped_tag_local ON CLUSTER cluster_3s_2r")
    ch_hook.execute(f"TRUNCATE TABLE xqc_ods.manual_task_local ON CLUSTER cluster_3s_2r")
    ch_hook.execute(f"ALTER TABLE xqc_ods.wiped_tag_new_local ON CLUSTER cluster_3s_2r DROP PARTITION {ds_nodash}")


def xinghuan_mc_qt_task_map(data):
    if not data:
        return []
    for key, value in data.items():
        if isinstance(value, list):
            temp = list()
            for it in value:
                if isinstance(it, ObjectId):
                    temp.append(str(it))
                else:
                    temp.append(it)
            data.update({key: temp})
    return [data]


def xdqc_wipe_record_map(data):
    if not data:
        return []
    dataList = list()
    wipe_messages = data.get("wipe_messages") or {}
    messages = wipe_messages.get("messages") or []
    template = deepcopy(data)
    template.update({
        "wipe_id": str(data.get("wiper_id") or ""),
        "wipe_time": data.get("wiper_time"),
        "dialog_id": wipe_messages.get("dialog_id"),
        "dialog_time": wipe_messages.get("begin_timestamp"),
    })
    if not messages:
        dataList.append(template)
    else:
        for msg in messages:
            temp = deepcopy(template)
            abnormal = msg.get("abnormals") or []
            excellents = msg.get("excellents") or []
            rule_scores = msg.get("rule_scores") or []
            emotion = msg.get("emotion") or {}

            temp.update({
                "messages_id": msg.get("id"),
                "abnormal_types": [str(i.get("type") or "") for i in abnormal],
                "abnormal_scores": [i.get("score") for i in abnormal],
                "excellents_types": [str(i.get("type") or "") for i in excellents],
                "excellents_scores": [i.get("score") for i in excellents],
                "rule_scores_ids": [str(i.get("id")) for i in rule_scores],
                "rule_scores_counts": [i.get("acount") for i in rule_scores],
                "rule_scores_scores": [i.get("score") for i in rule_scores],
                "emotion_type": str(emotion.get("type") or ""),
                "emotion_score": emotion.get("score"),
            })
            dataList.append(temp)
    return dataList


def xqc_wipe_record_map(data):
    if not data:
        return []
    messages_source = list()
    messages_content = list()
    messages_timestamp = list()
    create_time = data.get('create_time')
    day = create_time.strftime("%Y%m%d")
    data.update({
        "day": int(day)
    })
    if "messages" in data.keys():
        messages = data.get("messages")
        for message in messages:
            messages_source.append(message.get("source"))
            messages_content.append(message.get("content"))
            messages_timestamp.append(message.get("timestamp"))
        data.update({
            "messages_source": messages_source,
            "messages_content": messages_content,
            "messages_timestamp": messages_timestamp,
        })
    return [data]


def xqc_wipe_aggregate_query(ds_nodash):
    local_datetime = datetime.strptime(str(ds_nodash), "%Y%m%d")
    begin_utc_datetime = local_datetime - timedelta(hours=8)
    end_utc_datetime = begin_utc_datetime + timedelta(days=1)
    mongo_filter = [
        {"$match":
            {
                "create_time":
                    {
                        "$gte": begin_utc_datetime,
                        "$lt": end_utc_datetime
                    }

            }
        }
    ]
    return mongo_filter


def xinghuan_mc_qc_task_map(data):
    if not data:
        return []
    item = {**data}
    for key, value in data.items():
        if isinstance(value, list):
            temp = list()
            for it in value:
                if isinstance(it, ObjectId):
                    temp.append(str(it))
                else:
                    temp.append(it)
            item.update({key: temp})
        if key in ['cycle_date', 'dialog_date_range']:
            item.update({
                f"{key}_gte": value.get("gte") or 0,
                f"{key}_lte": value.get("lte") or 0,
            })
    return [item]


truncate_ch_tmp_table = PythonOperator(
    task_id="truncate_ch_tmp_table",
    python_callable=truncate_ch_tmp_table_func,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

sync_xqc_task_record_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xqc_task_record_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XQC_MONGO_DB,
    mongo_collection=TASK_RECORD_COLLECTION,
    destination_ch_table='xqc_ods.manual_task_record_all',
    aggregate_query=None,
    flatten_map=xqc_task_record_map,
    dag=dag
)

sync_xinghuan_mc_case_detail_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xinghuan_mc_case_detail_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XH_MONGO_DB,
    mongo_collection="case_detail",
    destination_ch_table='xqc_ods.xinghuan_mc_case_detail_all',
    aggregate_query=None,
    flatten_map=None,
    dag=dag
)

sync_xqc_appeal_task_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xqc_appeal_task_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XQC_MONGO_DB,
    mongo_collection="appeal_task",
    destination_ch_table='xqc_ods.xqc_appeal_task_all',
    aggregate_query=None,
    flatten_map=None,
    dag=dag
)

sync_xinghuan_mc_qt_task_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xinghuan_mc_qt_task_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XH_MONGO_DB,
    mongo_collection="qt_task",
    destination_ch_table='xqc_ods.xinghuan_mc_qt_task_all',
    aggregate_query=None,
    flatten_map=xinghuan_mc_qt_task_map,
    dag=dag
)

sync_xdqc_wipe_record = MongoToClickHouseOperator(
    task_id='sync_xdqc_wipe_record',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XDQC_MONGO_DB,
    mongo_collection=QC_WIPE_RECORD_COLLECTION,
    destination_ch_table='xqc_ods.wiped_tag_all',
    aggregate_query=None,
    flatten_map=xdqc_wipe_record_map,
    dag=dag
)

sync_xqc_wipe_record = MongoToClickHouseOperator(
    task_id='sync_xqc_wipe_record',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XQC_MONGO_DB,
    mongo_collection=QC_WIPE_RECORD_COLLECTION,
    destination_ch_table='xqc_ods.wiped_tag_new_all',
    aggregate_query=None,
    aggregate_func=xqc_wipe_aggregate_query,
    flatten_map=xqc_wipe_record_map,
    aggregate_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    dag=dag
)

sync_xinghuan_mc_qc_task = MongoToClickHouseOperator(
    task_id='sync_xinghuan_mc_qc_task',
    mongo_conn_id=MONGO_CONN_ID,
    clickhouse_conn_id=CLICKHOUSE_CONN_ID,
    mongo_db=XH_MONGO_DB,
    mongo_collection="qc_task",
    destination_ch_table='xqc_ods.manual_task_all',
    aggregate_query=None,
    flatten_map=xinghuan_mc_qc_task_map,
    dag=dag
)

truncate_ch_tmp_table >> [
    sync_xqc_task_record_to_ch_tmp,
    sync_xinghuan_mc_case_detail_to_ch_tmp,
    sync_xqc_appeal_task_to_ch_tmp,
    sync_xinghuan_mc_qt_task_to_ch_tmp,
    sync_xdqc_wipe_record,
    sync_xqc_wipe_record,
    sync_xinghuan_mc_qc_task,
]
