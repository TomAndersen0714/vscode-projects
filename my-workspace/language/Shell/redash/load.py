#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @Time    : 2019/11/26 14:51
# @Author  : xianghai
import json
import kudu
from bson import json_util
import pymongo
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
log = logging.getLogger("root")


def get_data():
    with open('./user_dept.json', mode='r', encoding='UTF-8') as fi:
        ret = json.load(fi, encoding='UTF-8')
    return ret


def sync_to_kudu(records):
    """

    :param records:
    :param day:
    :return:
    """
    # 连接到kudu主服务器
    client = kudu.connect(host='zjk-bigdata002', port=7051)
    # 打开表
    table = client.table('impala::dim.rde_user')
    session = client.new_session()

    log.info("sync: %d" % len(records))

    for record in records:
        op = table.new_upsert(record)
        session.apply(op)
    try:
        session.flush()
    except kudu.KuduBadStatus:
        logging.error(session.get_pending_errors())


if __name__ == '__main__':
    sync_to_kudu(get_data())


