-- 质检总览-数据监控-服务情况趋势及以上内容
https://xh-mc.xiaoduoai.com/api/v1/data_monitor/dialog
-- 示例数据
data: {
    total_count: 4482 // 会话总量
    avg_score: 102 // 质检平均分
    abnormal_count: 877 // 扣分会话
    abnormal_rate: 0.19567157518964748 // 扣分会话占比
    ai_abnormal_count: 877 // AI质检异常会话量
    high_ex_emotion_count: 109 // 高异常情绪案例数量
    high_emotion_dialogs: ["616a8576155364000171433f", …] // 高异常情绪案例
    human_check_count: 2 // 人工抽检量
    suggestion_check_count: 354 // 建议抽检量
    check_rate: 0.0004 // 抽检比例
    human_abnormal_count: 2 // 人工质检扣分量
    tendency_info: { // 服务情况趋势
        ai_subtract_score_proportion: 0.21621621621621623 // AI质检扣分
        day: 1634227200 // 日期
        manual_subtract_score_proportion: 0 // 人工质检扣分
        subtract_score_proportion: 0.21621621621621623 // 总扣分
    }
}
-- 后端查询
func BuildDialogInfoSql(startDate, endDate int64, platform string, shopName []string) string {
	var sqlBuilder strings.Builder
	shopStr := BuildShopStr(shopName)

	sqlBuilder.WriteString("select a.*, b.json_list as json_list from (SELECT platform, sum(session_count) as total_count, sum(subtract_score_count) as abnormal_count, sum(subtract_score_count) / sum(session_count) " +
		"as abnormal_rate, sum(ai_subtract_score_count) as ai_abnormal_cnt, sum(manual_qc_count) as human_check_count, toInt32(round((0.9604*sum(session_count))/(0.0025*sum(session_count)+0.9604), 0)) as suggestion_check_count, " +
		"round((sum(session_count) *100 +sum(ai_add_score) -sum(ai_subtract_score)) /sum(session_count),2) AS avg_score," +
		"round((sum(manual_qc_count) / sum(session_count)) ,4) as check_rate, sum(manual_subtract_score_count) as human_abnormal_count, length(arrayReduce('groupUniqArray',flatten(groupArray(high_abnormal_emo_list)))) as " +
		"high_ex_emotion_count, toString(arrayReduce('groupUniqArray',flatten(groupArray(high_abnormal_emo_list)))) as high_ex_emotion_dialog_id FROM ods.qc_session_count_all ",
	)
	sqlBuilder.WriteString(fmt.Sprintf("WHERE date between %d and %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("group by platform) as a left join (select platform,toString(groupArray(json_info)) as json_list from (SELECT platform, concat('{\"day\":',toString(toInt64(date)),',\"subtract_score_proportion\":'," +
		"toString((sum(subtract_score_count) / sum(session_count))),',\"manual_subtract_score_proportion\":', toString((sum(manual_subtract_score_count) / sum(session_count))),'," +
		"\"ai_subtract_score_proportion\":',toString((sum(ai_subtract_score_count) / sum(session_count))),'}') as json_info FROM ods.qc_session_count_all ",
	)
	sqlBuilder.WriteString(fmt.Sprintf("WHERE date between %d and %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("group by date,platform order by date) json group by platform) as b on a.platform = b.platform ")

	return sqlBuilder.String()
}
-- 查询SQL
select a.*,
    b.json_list as json_list
from (
        SELECT platform,
            sum(session_count) as total_count,
            sum(subtract_score_count) as abnormal_count,
            sum(subtract_score_count) / sum(session_count) as abnormal_rate,
            sum(ai_subtract_score_count) as ai_abnormal_cnt,
            sum(manual_qc_count) as human_check_count,
            toInt32(
                round(
                    (0.9604 * sum(session_count)) /(0.0025 * sum(session_count) + 0.9604),
                    0
                )
            ) as suggestion_check_count,
            round(
                (
                    sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
                ) / sum(session_count),
                2
            ) AS avg_score,
            round((sum(manual_qc_count) / sum(session_count)), 4) as check_rate,
            sum(manual_subtract_score_count) as human_abnormal_count,
            length(
                arrayReduce(
                    'groupUniqArray',
                    flatten(groupArray(high_abnormal_emo_list))
                )
            ) as high_ex_emotion_count,
            toString(
                arrayReduce(
                    'groupUniqArray',
                    flatten(groupArray(high_abnormal_emo_list))
                )
            ) as high_ex_emotion_dialog_id
        FROM ods.qc_session_count_all
        WHERE date between 1649260800 and 1649865599
            and platform = 'jd'
            and shop_name in ('智晓多谋水果专营店')
        group by platform
    ) as a
    left join (
        select platform,
            toString(groupArray(json_info)) as json_list
        from (
                SELECT platform,
                    concat(
                        '{\"day\":',
                        toString(toInt64(date)),
                        ',\"subtract_score_proportion\":',
                        toString((sum(subtract_score_count) / sum(session_count))),
                        ',\"manual_subtract_score_proportion\":',
                        toString(
                            (
                                sum(manual_subtract_score_count) / sum(session_count)
                            )
                        ),
                        ',\"ai_subtract_score_proportion\":',
                        toString(
                            (
                                sum(ai_subtract_score_count) / sum(session_count)
                            )
                        ),
                        '}'
                    ) as json_info
                FROM ods.qc_session_count_all
                WHERE date between 1649260800 and 1649865599
                    and platform = 'jd'
                    and shop_name in ('智晓多谋水果专营店')
                group by date,
                    platform
                order by date
            ) json
        group by platform
    ) as b on a.platform = b.platform



-- 质检总览-AI质检问题TOP10, 人工质检问题TOP10, 质检词触发次数TOP10
https://xh-mc.xiaoduoai.com/api/v1/data_monitor/qc
-- 示例数据
data: {
    // AI质检问题
    ai_qc: {
        name: 单句响应慢 // 质检项
        rate: 0.3477 // 质检项占比
    }
    // 人工质检问题
    human_qc: {
        name: 淘宝售前 / 默认分类 / 错别字 // 质检项
        rate: 0.3 // 质检项占比
    }

    // 质检词触发分布
    qc_word: {
        name: 抱歉 // 质检项
        rate: 0.0013 // 质检项占比
    }
}
-- 后端查询
func BuildQcSql(startDate, endDate int64, platform string, shopName []string) string {
	var sqlBuilder strings.Builder
	shopStr := BuildShopStr(shopName)

	sqlBuilder.WriteString("select a.platform as platform ,type,b.qc_id as qc_id,b.qc_name as qc_name , round(b.count_info / a.count_all_info ,4) as qc_proportion from (select platform, sum(qc_count) as count_all_info from  ods.qc_question_detail_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" WHERE date >= %d and date < %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("and (`type` = 'ai' OR (`type` = 's_emotion') OR (`type` = 'c_emotion' AND qc_id>='4')) group by platform) as a left join (select platform,`type`, qc_id, qc_name, sum(qc_count) as count_info from ods.qc_question_detail_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" WHERE date >= %d and date < %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("and (`type` = 'ai' OR (`type` = 's_emotion') OR (`type` = 'c_emotion' AND qc_id>='4')) group by platform,`type`,qc_id,qc_name order by count_info desc limit 10) as b on a.platform = b.platform order by qc_proportion desc limit 10 ")

	sqlBuilder.WriteString(" UNION ALL ")

	sqlBuilder.WriteString("select a.platform as platform,'manual' as `type`,b.qc_id as qc_id,b.qc_name_all as qc_name, round(b.count_info / a.count_all_info ,4) as qc_proportion from (select platform, sum(qc_count) as count_all_info from ods.qc_question_detail_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" WHERE date >= %d and date < %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("and `type` = 'manual' group by platform,`type`) as a left join  (select platform,`type`, qc_id, replaceAll(replaceAll(qc_name,'未设置一级标签/',''),'未设置二级标签/','') as qc_name_all,sum(qc_count) as count_info from ods.qc_question_detail_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" WHERE date >= %d and date < %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("and `type` = 'manual' group by platform,`type`, qc_id,qc_name order by count_info desc limit 10) as b on a.platform = b.platform order by qc_proportion desc limit 10 ")

	sqlBuilder.WriteString(" UNION ALL ")

	sqlBuilder.WriteString("select b.platform as platform, 'qc_word' as `type`, '' as qc_id, a.word as qc_name, round((a.words_count_info/b.words_count_all),4) as qc_proportion from (select platform, word, sum(words_count) as words_count_info from ods.qc_words_detail_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" WHERE date >= %d and date < %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("group by platform,word order by words_count_info desc) a left join (select platform, sum(words_count) as words_count_all from ods.qc_words_detail_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" WHERE date >= %d and date < %d and platform = '%s' and shop_name in %s ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("group by platform order by words_count_all desc limit 10) b on a.platform = b.platform order by qc_proportion desc limit 10 ")

	return sqlBuilder.String()
}
-- 大数据查询
select a.platform as platform,
    type,
    b.qc_id as qc_id,
    b.qc_name as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= 1649260800
            and date < 1649865599
            and platform = 'jd'
            and shop_name in (
                '方太官方旗舰店',
                '方太京东自营旗舰店',
                '方太厨卫旗舰店',
                '方太烟灶旗舰店',
                '方太京东旗舰店',
                '方太集成烹饪中心京东自营旗舰店'
            )
            and (
                `type` = 'ai'
                OR (`type` = 's_emotion')
                OR (
                    `type` = 'c_emotion'
                    AND qc_id >= '4'
                )
            )
        group by platform
    ) as a
    left join (
        select platform,
            `type`,
            qc_id,
            qc_name,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= 1649260800
            and date < 1649865599
            and platform = 'jd'
            and shop_name in (
                '方太官方旗舰店',
                '方太京东自营旗舰店',
                '方太厨卫旗舰店',
                '方太烟灶旗舰店',
                '方太京东旗舰店',
                '方太集成烹饪中心京东自营旗舰店'
            )
            and (
                `type` = 'ai'
                OR (`type` = 's_emotion')
                OR (
                    `type` = 'c_emotion'
                    AND qc_id >= '4'
                )
            )
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info desc
        limit 10
    ) as b on a.platform = b.platform
order by qc_proportion desc
limit 10

UNION ALL
select a.platform as platform,
    'manual' as `type`,
    b.qc_id as qc_id,
    b.qc_name_all as qc_name,
    round(b.count_info / a.count_all_info, 4) as qc_proportion
from (
        select platform,
            sum(qc_count) as count_all_info
        from ods.qc_question_detail_all
        WHERE date >= 1649260800
            and date < 1649865599
            and platform = 'jd'
            and shop_name in (
                '方太官方旗舰店',
                '方太京东自营旗舰店',
                '方太厨卫旗舰店',
                '方太烟灶旗舰店',
                '方太京东旗舰店',
                '方太集成烹饪中心京东自营旗舰店'
            )
            and `type` = 'manual'
        group by platform,
            `type`
    ) as a
    left join (
        select platform,
            `type`,
            qc_id,
            replaceAll(replaceAll(qc_name, '未设置一级标签/', ''), '未设置二级标签/', '') as qc_name_all,
            sum(qc_count) as count_info
        from ods.qc_question_detail_all
        WHERE date >= 1649260800
            and date < 1649865599
            and platform = 'jd'
            and shop_name in (
                '方太官方旗舰店',
                '方太京东自营旗舰店',
                '方太厨卫旗舰店',
                '方太烟灶旗舰店',
                '方太京东旗舰店',
                '方太集成烹饪中心京东自营旗舰店'
            )
            and `type` = 'manual'
        group by platform,
            `type`,
            qc_id,
            qc_name
        order by count_info desc
        limit 10
    ) as b on a.platform = b.platform
order by qc_proportion desc
limit 10

UNION ALL
select b.platform as platform,
    'qc_word' as `type`,
    '' as qc_id,
    a.word as qc_name,
    round((a.words_count_info / b.words_count_all), 4) as qc_proportion
from (
    select platform,
        word,
        sum(words_count) as words_count_info
    from ods.qc_words_detail_all
    WHERE date >= 1649260800
        and date < 1649865599
        and platform = 'jd'
        and shop_name in (
            '方太官方旗舰店',
            '方太京东自营旗舰店',
            '方太厨卫旗舰店',
            '方太烟灶旗舰店',
            '方太京东旗舰店',
            '方太集成烹饪中心京东自营旗舰店'
        )
    group by platform, word
    order by words_count_info desc
) a
left join (
    select platform,
        sum(words_count) as words_count_all
    from ods.qc_words_detail_all
    WHERE date >= 1649260800
        and date < 1649865599
        and platform = 'jd'
        and shop_name in (
            '方太官方旗舰店',
            '方太京东自营旗舰店',
            '方太厨卫旗舰店',
            '方太烟灶旗舰店',
            '方太京东旗舰店',
            '方太集成烹饪中心京东自营旗舰店'
        )
    group by platform
    order by words_count_all desc
    limit 10
) b
on a.platform = b.platform
order by qc_proportion desc
limit 10



-- 质检总览-客服服务质量排行TOP10, 客服被检量排行TOP10, 质检人员质检量排行TOP10
https://xh-mc.xiaoduoai.com/api/v1/data_monitor/customer
-- 示例数据
data: {
    check: { // 质检人员质检量 
        name: "李晓婷" // 姓名
        average_check: 0.2857142857142857 // 日均质检量
        total_check: 2 // 质检总量
    }
    checked: { // 客服被检量排行
        average_check: 0.2857142857142857 // 日均质检量
        check_dialog: 2 // 抽检总量
        employee_name: "戴佳莉" // 人员姓名
        total_dialog: 44 // 会话总量
    }
    quality: { // 客服服务质量排行
        abnormal_rate: 0.05 // 扣分比例
        ai_abnormal: 20 // AI扣分
        avg_score: 112.44 // 平均分
        employee_name: "王颖"
        human_abnormal: 0 // 人工扣分
        total_abnormal: 20 // 总扣分
        total_dialog: 180 // 会话总量
        user_rule_score: 0 // 自定义扣分
    }
}
-- 后端查询
func BuildCustomerServiceRelationSql(startDate int64, endDate int64, platform string, shopName []string) string {
	var sqlBuilder strings.Builder
	shopStr := BuildShopStr(shopName)

	sqlBuilder.WriteString("select 'server' as type, employee_id, employee_name, sum(session_count) as total_count, 0 as total_check, sum(ai_subtract_score) as abnormal_score, " +
		"sum(subtract_score_count) / sum(session_count)  as abnormal_rate, sum(ai_subtract_score)- sum(manual_subtract_score)-sum(rule_score) as ai_abnormal_score, sum(manual_subtract_score) as human_abnormal_score, 0 as human_total_check, 0 " +
		"as average_check, sum(rule_score) AS user_rule_score, round((sum(session_count) *100 +sum(ai_add_score) -sum(ai_subtract_score)) /sum(session_count),2) AS avg_score from ods.qc_session_count_all ")
	sqlBuilder.WriteString(fmt.Sprintf(" where date >= %d and date < %d and platform = '%s' and shop_name in %s and employee_name != '' ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("group by employee_id, employee_name order by avg_score desc limit 10")

	sqlBuilder.WriteString(" union all ")

	sqlBuilder.WriteString("select 'server_read_mark' as type, employee_id, employee_name, sum(session_count) " +
		"as total_count, 0 as total_check, 0 as abnormal_score, 0 as abnormal_rate, 0 as ai_abnormal_score, 0 as human_abnormal_score, sum(manual_qc_count) as human_total_check, sum(manual_qc_count)/if(dateDiff('day', ")
	sqlBuilder.WriteString(fmt.Sprintf("toDate(%d), toDate(%d))=0,1, dateDiff('day', toDate(%d), toDate(%d))) as average_check, 0 as user_rule_score, 0 as avg_score from ods.qc_session_count_all ", startDate, endDate, startDate, endDate))
	sqlBuilder.WriteString(fmt.Sprintf(" where date >= %d and date < %d and platform = '%s' and shop_name in %s and employee_name != '' and manual_qc_count != 0 ", startDate, endDate, platform, shopStr))
	sqlBuilder.WriteString("group by employee_id, employee_name order by human_total_check desc limit 10")

	sqlBuilder.WriteString(" union all ")

	sqlBuilder.WriteString("select 'read_mark' as type, account_id as employee_id, username as employee_name, 0 as total_count, " +
		"count(1) as total_check, 0 as abnormal_score, 0 as abnormal_rate, 0 as ai_abnormal_count, 0 as human_abnormal_count, 0 as human_total_check, ")
	sqlBuilder.WriteString(fmt.Sprintf("count(1)/if(dateDiff('day', toDate(%d), toDate(%d))=0,1, dateDiff('day', toDate(%d), toDate(%d))) as average_check, 0 as user_rule_score, 0 as avg_score from ods.qc_read_mark_detail_all ", startDate, endDate, startDate, endDate))
	sqlBuilder.WriteString(fmt.Sprintf(" where date >= %d and date < %d and platform = '%s' and shop_name in %s and username != '' and employee_name != '' group by account_id,username order by total_check desc limit 10", startDate, endDate, platform, shopStr))

	return sqlBuilder.String()
}
-- 大数据查询
select 'server' as type,
    employee_id,
    employee_name,
    sum(session_count) as total_count,
    0 as total_check,
    sum(ai_subtract_score) as abnormal_score,
    sum(subtract_score_count) / sum(session_count) as abnormal_rate,
    sum(ai_subtract_score) - sum(manual_subtract_score) - sum(rule_score) as ai_abnormal_score,
    sum(manual_subtract_score) as human_abnormal_score,
    0 as human_total_check,
    0 as average_check,
    sum(rule_score) AS user_rule_score,
    round(
        (
            sum(session_count) * 100 + sum(ai_add_score) - sum(ai_subtract_score)
        ) / sum(session_count),
        2
    ) AS avg_score
from ods.qc_session_count_all
where date >= 1649260800
    and date < 1649865600
    and platform = 'jd'
    and shop_name in ('智晓多谋水果专营店')
    and employee_name != ''
group by employee_id,
    employee_name
order by avg_score desc
limit 10
union all
select 'server_read_mark' as type,
    employee_id,
    employee_name,
    sum(session_count) as total_count,
    0 as total_check,
    0 as abnormal_score,
    0 as abnormal_rate,
    0 as ai_abnormal_score,
    0 as human_abnormal_score,
    sum(manual_qc_count) as human_total_check,
    sum(manual_qc_count) / if(
        dateDiff('day', toDate(1649260800), toDate(1649865600)) = 0,
        1,
        dateDiff('day', toDate(1649260800), toDate(1649865600))
    ) as average_check,
    0 as user_rule_score,
    0 as avg_score
from ods.qc_session_count_all
where date >= 1649260800
    and date < 1649865600
    and platform = 'jd'
    and shop_name in ('智晓多谋水果专营店')
    and employee_name != ''
    and manual_qc_count != 0
group by employee_id,
    employee_name
order by human_total_check desc
limit 10
union all
select 'read_mark' as type,
    account_id as employee_id,
    username as employee_name,
    0 as total_count,
    count(1) as total_check,
    0 as abnormal_score,
    0 as abnormal_rate,
    0 as ai_abnormal_count,
    0 as human_abnormal_count,
    0 as human_total_check,
    count(1) / if(
        dateDiff('day', toDate(1649260800), toDate(1649865600)) = 0,
        1,
        dateDiff('day', toDate(1649260800), toDate(1649865600))
    ) as average_check,
    0 as user_rule_score,
    0 as avg_score
from ods.qc_read_mark_detail_all
where date >= 1649260800
    and date < 1649865600
    and platform = 'jd'
    and shop_name in ('智晓多谋水果专营店')
    and username != ''
    and employee_name != ''
group by account_id,
    username
order by total_check desc
limit 10