from datetime import datetime

TIME_FM = '%Y-%m-%d %H:%M:%S.%f'
TIME_FM_NO_DOT = '%Y-%m-%d %H:%M:%S'


# 时间格式化函数，去掉+8和T，考虑到+8
# 格式化分为带小数点和不带的
def time_format(time_str):
    standard_time_str = time_str.replace("T", " ").replace("+08:00", "")
    if '.' in standard_time_str:
        standard_time_str = standard_time_str[:26] if len(standard_time_str) > 26 else standard_time_str
        dt = datetime.strptime(standard_time_str, TIME_FM)
    else:
        dt = datetime.strptime(standard_time_str, TIME_FM_NO_DOT)
    return dt.strftime(TIME_FM)


def parse(dic, logger):
    # "2020-10-22T21:21:55.233825142+08:00"
    if dic.get("status", "") in ("TradeChanged", "TradeMemoModified", "TradeModifyAddress", "TradeModifyFee"):
        return []
    try:
        trade_dict = dic.get('trade', {})
        modified = trade_dict.get("modified", '1970-01-01 00:00:00.000')
        modified = time_format(modified)
        # if "+08:00" not in modified and len(modified) >= 19:
        #     modified = modified[0:19] + ".000000000+08:00"
    except Exception as e:
        logger.info(str(e))
        logger.info("error data " + str(dic))
        return []
    if not trade_dict:
        return []
    inner_status = str(trade_dict.get('status', ''))
    if dic.get("status", "") == "shipped":
        inner_status = 'WAIT_BUYER_CONFIRM_GOODS'
    if dic.get("status", "") == "part_shipped":
        inner_status = 'part_shipped'
    res_dic = dict(
        buyer_nick=str(trade_dict.get('buyer_nick', '')),
        #新增脱敏字段real_buyer_nick 2022-08-04
        # by:yangboxiao@xiaoduotech.com
        real_buyer_nick=dic.get("real_buyer_nick", ""),
        iid=str(trade_dict.get('iid', '')),
        modified=modified,
        oid=str(trade_dict.get('oid', '')),
        pay_ment=int(float(trade_dict.get('payment', 0)) * 100),
        post_fee=str(trade_dict.get('post_fee', '')),
        seller_nick=str(trade_dict.get('seller_nick', '')),
        status=inner_status,
        store_code=str(trade_dict.get('store_code', '')),
        tid=str(trade_dict.get('tid', '')),
        type=str(trade_dict.get('type', '')),
        day=int(modified[0:10].replace("-", "")),
    )
    return [res_dic]