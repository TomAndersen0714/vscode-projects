from datetime import datetime
import traceback


def check_is_identified(dic, question_b):
    if question_b:
        qid = question_b.get('qid', 0)
        proba = float(question_b.get('proba', 0))
        if qid == 0 or proba < 0.5:
            return 0
        else:
            return 1
    if dic.get('intent') and isinstance(dic.get('intent'), list):
        intent = dic.get('intent')
        if len(intent) > 1:
            if intent[0][0] == 0 or intent[0][1] < 0.5:
                return 0
            else:
                return 1
    return 0


def parse(dic, logger):
    prob_b = dic.get('question_b', {})
    transfer = dic.get('transfer', {})
    shop_question = dic.get('shop_question', {})
    # 如果是未回复日志，则过滤掉
    log_msg_type = dic.get('log_msg_type', '')
    if log_msg_type == 'no_reply':
        return []
    # 判断该消息是否被识别
    try:
        is_identified = check_is_identified(dic, prob_b)
    except Exception as e:
        is_identified = 0
    # 获取嵌套json字段
    if not isinstance(prob_b, dict):
        prob_b = dict()
    if not isinstance(transfer, dict):
        transfer = dict()
    if not isinstance(shop_question, dict):
        shop_question = dict()
    try:
        time_str = (dic.get("create_time", '2000-01-01 00:00:00.000000'))
        create_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        logger.info(str(e))
        logger.info(traceback.format_exc())
        return []
    tmp_mode = str(dic.get('mode', ''))
    tmp_snick = str(dic.get('snick', ''))
    # 过滤掉测试cnick数据
    tmp_cnick = str(dic.get('cnick', ''))
    for x in ('comxiaoduoSearch', 'comxdAnonymousCustomer', 'comxiaoduo'):
        if x in tmp_cnick:
            return []

    # 主账号，如果没有冒号，则加入相同的名字，例如cntaobao美的旗舰店，则变更为cntaobao美的旗舰店:美的旗舰店
    res_snick = tmp_snick if ':' in tmp_snick or tmp_snick == '' \
        else tmp_snick + ':' + tmp_snick.replace("cntaobao", "")
    res_dic = dict(
        mp_category=str(dic.get('mp_category', '')),
        plat_goods_id=str(dic.get('plat_goods_id', '')),
        shop_id=str(dic.get('shop_id', '')),
        snick=res_snick,
        answer_explain=str(dic.get('answer_explain', '')),
        create_time=create_time,
        question_type=int(dic.get('question_type', 0)),
        remind_answer=str(dic.get('remind_answer', '')),
        cnick=tmp_cnick,
        mode=tmp_mode,
        msg=str(dic.get('msg', '')),
        robot_answer=str(dic.get('robot_answer', '')),
        act=str(dic.get('act', '')),
        intent=str(dic.get('intent', '')),
        msg_id=str(dic.get('msgid', '')),
        task_id=str(dic.get("task_id", "")),
        platform=str(dic.get('platform', '')),
        msg_time=int(dic.get('msg_time', 0)),
        qa_id=str(dic.get('qa_id', '')),
        question_b_qid=str(prob_b.get('qid', '')),
        question_b_proba=str(prob_b.get('proba', '')),
        question_b_standard_q=str(prob_b.get('standard_q', '')),
        is_identified=is_identified,
        current_sale_stage=str(dic.get('current_sale_stage', '')),
        transfer_from=str(transfer.get('from', '')),
        transfer_to=str(transfer.get('to', '')),
        transfer_type=str(transfer.get('transfer_type', '')),
        send_msg_from=int(dic.get('send_msg_from', 0)),
        answer_id=str(dic.get('answer_id', '')),
        mp_version=int(dic.get('mp_version', 0)),
        ms_msg_time=int(dic.get('ms_msg_time', 0)),
        shop_question_type=str(shop_question.get('type', '')),
        shop_question_id=str(shop_question.get('id', '')),
        no_reply_reason=int(dic.get("no_reply_reason", -1)),
        no_reply_sub_reason=int(dic.get("no_reply_sub_reason", -1)),
        precise_intent_id=str(prob_b.get('precise_intent_id', '')),
        precise_intent_standard_q=str(prob_b.get('precise_intent_standard_q', '')),
        msg_scenes_source=str(dic.get('msg_scenes_source', '')),
        msg_content_type=str(dic.get('msg_content_type', '')),
        trace_id=str(dic.get('trace_id', '')),
        day=int(time_str[0:10].replace("-", "")),
        #question_b_id=str(dic.get('question_b_id', '')),
        cond_answer_id=str(dic.get('cond_answer_id', ''))
    )
    return [res_dic]

