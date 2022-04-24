-- 产品数据-明察-页面使用情况
-- 淘宝: clickhouse
WITH xqc_company_shop_info AS (
    -- 查询XQC各客户店铺信息
    SELECT
        company_id,
        groupArray(platform) AS platforms,
        groupArray(shop_cnt) AS shop_cnts,
        groupArray(shop_ids) AS shop_ids_arr,
        groupArray(seller_nicks) AS seller_nicks_arr
    FROM (
        SELECT
            company_id,
            platform,
            count(1) AS shop_cnt,
            groupArray(seller_nick) AS seller_nicks,
            groupArray(shop_id) AS shop_ids
        FROM xqc_dim.xqc_shop_all
        WHERE `day` = toYYYYMMDD(yesterday())
        GROUP BY company_id, platform
    ) AS company_platform_info
    GROUP BY company_id
),
xqc_company_duration_info AS (
    -- 查询XQC各客户到期信息
    SELECT
        _id AS company_id,
        name,
        shot_name,
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
        groupArray(role_name) AS versions
    FROM dim.pri_center_version_all
    WHERE product_name = '明察质检(XQC)'
        AND user_id NOT in ('60f957f1c3d62bccb1606bd9', '方太', '测试')
    GROUP BY shot_name
),
xqc_page_mark_label_info AS (
    -- 查询前端埋点页面标签信息
    SELECT DISTINCT
        `url` AS url_path,
        marked_content AS url_label
    FROM dim.burying_point_marked_all
    WHERE event = '$pageview'
    AND app_id = 'xd001'
),
xqc_company_info AS (
    -- 关联客户到期信息, 版本信息, 店铺信息
    SELECT *
    FROM (
        SELECT *
        FROM xqc_company_duration_info
        GLOBAL INNER JOIN xqc_company_shop_info
        USING (company_id)
    ) AS xqc_company_duration_shop_info
    GLOBAL INNER JOIN xqc_company_version_info
    USING(shot_name)
),
xqc_company_page_view_stat AS (
    -- 统计各客户所有页面访问情况
    SELECT
        shot_name,
        groupArray(url_label) AS url_labels,
        groupArray(url_path) AS url_paths,
        groupArray(url_pv) AS url_pvs
    FROM (
        -- 统计各客户各页面访问情况
        SELECT
            arrayElement(splitByString(':', distinct_id), 1) AS shot_name,
            url_path,
            -- uniqExact(arrayElement(splitByString(':', distinct_id), 2)) AS uv,
            count(1) AS url_pv
        FROM ods.web_log_dis
        WHERE `day` BETWEEN toYYYYMMDD(toDate('{{ day.end=yesterday }}')) AND toYYYYMMDD(toDate('{{ day.start=month_ago }}'))
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
    ) AS company_url_stat
    GLOBAL LEFT JOIN xqc_page_mark_label_info
    USING(url_path)
    GROUP BY shot_name
)
-- 关联客户页面访问情况, 埋点标注信息, 到期信息, 版本信息, 店铺信息
SELECT
    company_id,
    name,
    shot_name AS short_name,
    customer_type,
    create_date,
    expire_date,
    remain_days,
    service_days,
    platforms,
    shop_cnts,
    shop_ids_arr,
    seller_nicks_arr,
    versions,
    url_labels,
    url_paths,
    url_pvs
FROM xqc_company_page_view_stat
GLOBAL RIGHT JOIN xqc_company_info
USING(shot_name)
WHERE shot_name NOT IN ('测试', '测试:测试') 
ORDER BY shot_name ASC COLLATE 'zh'