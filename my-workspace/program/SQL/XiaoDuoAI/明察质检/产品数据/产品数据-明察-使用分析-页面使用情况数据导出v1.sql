-- 产品数据-明察-页面使用情况
-- 淘宝: clickhouse
WITH xqc_company_shop_cnt AS (
    -- 统计XQC各客户店铺数
    SELECT
        company_id AS _id,
        count(1) AS shop_cnt
    FROM xqc_dim.xqc_shop_all
    WHERE `day` = toYYYYMMDD(yesterday())
    GROUP BY _id
),
xqc_company_duration_info AS (
    -- 查询XQC各客户到期信息
    SELECT _id,
        name,
        shot_name,
        platforms,
        IF (
            dateDiff('day', create_date, expire_date) >= 40,
            '正式',
            '试用'
        ) AS customer_type,
        toDate(create_time) AS create_date,
        toDate(expire_time) AS expire_date,
        dateDiff('day', today(), expire_date) AS remain_days,
        dateDiff('day', create_date, expire_date) AS service_days
    FROM xqc_dim.company_all
    WHERE shot_name NOT IN (
            '何相玄',
            '测试',
            '客户端'
        )
        AND expire_date >= toDate(subtractWeeks(yesterday(), 1))
        AND `day` = toYYYYMMDD(yesterday())
),
xqc_company_version_info AS (
    -- 查询客户开通版本信息
    SELECT user_name AS shot_name,
        arrayStringConcat(groupArray(role_name), ',') AS versions
    FROM dim.pri_center_version_all
    WHERE product_name = '明察质检(XQC)'
        AND user_id NOT in ('60f957f1c3d62bccb1606bd9', '方太', '测试')
    GROUP BY shot_name
),
xqc_page_mark_info AS (
    -- 查询前端埋点标注信息
    SELECT DISTINCT url,
        marked_content
    FROM dim.burying_point_marked_all
    WHERE event = '$pageview'
    AND app_id = 'xd001'
),
xqc_company_info AS (
    -- 关联客户到期信息, 版本信息, 店铺数量
    SELECT *
    FROM (
        SELECT *
        FROM xqc_company_duration_info
        GLOBAL INNER JOIN xqc_company_shop_cnt
        USING (_id)
    ) AS xqc_company_duration_shop_cnt
    GLOBAL INNER JOIN xqc_company_version_info
    USING(shot_name)
),
xqc_company_page_view_stat AS (
    -- 统计各客户各个页面访问情况
    SELECT
        arrayElement(splitByString(':', distinct_id), 1) AS shot_name,
        url_path,
        -- uniqExact(arrayElement(splitByString(':', distinct_id), 2)) AS uv,
        count(1) AS pv
    FROM ods.web_log_dis
    WHERE `day` BETWEEN toYYYYMMDD(toDate('{{ day.end }}')) AND toYYYYMMDD(toDate('{{ day.start }}'))
        -- 过滤页面查看动作
        AND `event` = '$pageview'
        -- 过滤明察质检url
        AND url LIKE '%xh-mc.xiaoduoai.com/%'
        -- 过滤单店和多店
        AND app_id IN ('xd001', 'xd023')
        AND shot_name GLOBAL IN (
            SELECT shot_name
            FROM xqc_dim.company_all
            WHERE day = toYYYYMMDD(yesterday())
        )
    GROUP BY shot_name, url_path
)
-- 关联客户页面访问情况,埋点标注信息,到期信息, 版本信息, 店铺数量
SELECT
    xqc_company_info._id,
    xqc_company_info.name,
    xqc_company_info.shot_name AS short_name,
    xqc_company_info.versions,
    xqc_company_info.platforms,
    xqc_company_info.shop_cnt,
    xqc_company_info.customer_type,
    xqc_company_info.create_date AS `开通日期`,
    xqc_company_info.expire_date AS `过期日期`,
    xqc_company_info.service_days AS `合同有效期`,
    xqc_company_info.remain_days AS `剩余有效期`,
    xqc_company_stat.marked_content AS `模块`,
    xqc_company_stat.url_path,
    xqc_company_stat.pv
FROM (
    SELECT
        xqc_company_page_view_stat.*,
        xqc_page_mark_info.marked_content
    FROM xqc_page_mark_info
    GLOBAL LEFT JOIN xqc_company_page_view_stat
    ON xqc_page_mark_info.url = xqc_company_page_view_stat.url_path
) AS xqc_company_stat
GLOBAL RIGHT JOIN xqc_company_info
USING(shot_name)
WHERE shot_name NOT IN ('测试', '测试:测试')
ORDER BY short_name ASC COLLATE 'zh'