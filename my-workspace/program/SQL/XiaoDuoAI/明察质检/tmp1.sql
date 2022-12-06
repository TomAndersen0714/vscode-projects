-- 淘宝: clickhouse
-- 明察开通客户的周启用率
-- 时间范围选择：周期初——周期末
WITH _day AS (
    SELECT toDate(`day`) AS `day`
    FROM utils.day_all
    WHERE `day` BETWEEN '{{ day.start }}' AND '{{ day.end }}'
        AND dayofweek(`day`) = 4
),
_week_day AS (
    SELECT a.day,
        toDate(b.day) AS week_day
    FROM _day a
        CROSS JOIN utils.day_all b
    WHERE week_day BETWEEN subtractDays(a.`day`, 6) AND a.day
),
cus AS (
    SELECT DISTINCT company_id,
        customer_name,
        customer_no
    FROM xqc_dim.xqc_shop_all
        JOIN (
            SELECT DISTINCT customer_no,
                customer_name,
                shop_id
            FROM dws.crm_shop_contract_all
        ) USING(shop_id)
    WHERE `day` = toYYYYMMDD(toDate('{{ day.end }}'))
),
xqc_company_duration_info AS (
    -- 查询XQC各客户到期信息
    SELECT DISTINCT `day`,
        _id AS company_id,
        name,
        toDate(create_time) AS create_date,
        toDate(expire_time) AS expire_date,
        shot_name
    FROM xqc_dim.company_all
    WHERE shot_name NOT IN (
            '何相玄',
            '测试',
            '测试:测试',
            '客户端',
            '宝尊'
        )
        AND `day` BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}'))
        AND dateDiff('day', create_date, expire_date) >= 40 --正式版客户
        AND `day` BETWEEN toYYYYMMDD(create_date) AND toYYYYMMDD(expire_date) --每日在期客户
),
xqc_company_page_view_stat AS (
    -- 统计各客户各页面访问情况
    SELECT arrayElement(splitByString(':', distinct_id), 1) AS shot_name,
        `day`,
        -- url_path,
        -- uniqExact(arrayElement(splitByString(':', distinct_id), 2)) AS uv,
        count(1) AS url_pv
    FROM ods.web_log_dis
    WHERE `day` BETWEEN toYYYYMMDD(toDate('{{ day.start }}')) AND toYYYYMMDD(toDate('{{ day.end }}')) -- 过滤页面查看动作
        AND `event` = '$pageview' -- 过滤明察质检url
        AND url LIKE '%xh-mc.xiaoduoai.com/%' -- 过滤单店和多店
        AND app_id IN ('xd001', 'xd023')
    GROUP BY shot_name,
        `day`
) -- 关联客户页面访问情况, 埋点标注信息, 到期信息, 版本信息, 店铺信息
,
mer AS (
    SELECT company_id,
        name,
        shot_name AS short_name,
        `day`,
        url_pv
    FROM xqc_company_page_view_stat GLOBAL
        RIGHT JOIN xqc_company_duration_info USING(
            shot_name,
            `day`
        )
),
mer1 AS (
    SELECT customer_no,
        customer_name,
        mer.*
    FROM mer
        LEFT JOIN cus USING(company_id)
),
mer2 AS (
    SELECT mer1.*,
        _week_day.`day` AS w
    FROM mer1
        JOIN _week_day ON mer1.day = toYYYYMMDD(_week_day.week_day)
)
SELECT customer_no AS "客户编号",
    customer_name AS "纷享销客客户名",
    company_id AS "明察后台客户编号",
    name AS "明察后台客户名称",
    short_name AS "明察后台客户简称",
    `day` AS "日期",
    url_pv AS pv,
    w AS "周"
FROM mer2