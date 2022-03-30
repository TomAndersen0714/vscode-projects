-- 质检报表-客服-查看详情-人工质检结果
-- 统计维度: 平台/店铺/子账号, 下钻维度路径: 平台/店铺/子账号分组/子账号/会话
SELECT
    CASE
        WHEN platform='tb' THEN '淘宝'
        WHEN platform='jd' THEN '京东'
        WHEN platform='ks' THEN '快手'
        WHEN platform='dy' THEN '抖音'
        WHEN platform='pdd' THEN '拼多多'
        WHEN platform='open' THEN '开放平台'
        ELSE platform
    END AS `平台`,
    seller_nick AS `店铺`,
    department_name AS `子账号分组`,
    snick AS `客服子账号`,
    employee_name AS `客服姓名`,
    superior_name AS `上级姓名`,
    
    -- 人工质检结果
    sumMap(human_check_tag_name_arr, human_check_tag_cnt_arr) AS human_check_tag_cnt_kvs,
    arrayStringConcat(arrayMap(x->toString(x),human_check_tag_cnt_kvs.1),'$$') AS `人工质检标签`,
    arrayStringConcat(arrayMap(x->toString(x),human_check_tag_cnt_kvs.2),'$$') AS `人工质检触发次数`

FROM (
    -- 人工质检结果-子账号维度
    SELECT
        platform,
        seller_nick,
        snick,
        groupArray(tag_name) AS human_check_tag_name_arr,
        groupArray(tag_cnt) AS human_check_tag_cnt_arr
    FROM (
        -- 人工质检-子账号维度扣分标签触发次数统计
        SELECT
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_score_stat_count + tag_score_stat_md) AS tag_cnt
        FROM (
            -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
            SELECT
                platform,
                seller_nick,
                snick,
                tag_score_stats_id AS tag_score_stats_id,
                -- 缺失历史数据直接为0, 对齐数组长度
                if(
                    length(tag_score_stats_count)!=length(tag_score_stats_id),
                    arrayResize([0],length(tag_score_stats_id),0),
                    tag_score_stats_count
                ) AS tag_score_stats_count,
                if(
                    length(tag_score_stats_md)!=length(tag_score_stats_id),
                    arrayResize([0],length(tag_score_stats_id),0),
                    tag_score_stats_md
                ) AS tag_score_stats_md
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND snick GLOBAL IN (
                -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            -- 清除没有打标的数据, 减小计算量
            AND tag_score_stats_id!=[]
            -- 下拉框-店铺名
            AND (
                    '{{ seller_nicks }}'=''
                    OR
                    seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
            -- 下拉框-子账号
            AND (
                    '{{ snicks=null }}'=''
                    OR
                    snick IN splitByChar(',','{{ snicks=null }}')
            )
        ) AS transformed_dialog_info
        ARRAY JOIN
            tag_score_stats_id AS tag_id,
            tag_score_stats_count AS tag_score_stat_count,
            tag_score_stats_md AS tag_score_stat_md
        -- 清除空数据
        WHERE tag_score_stats_id!=[]
        GROUP BY platform, seller_nick, snick, tag_id
        
        UNION ALL
        
        -- 人工质检-子账号维度加分标签触发次数统计
        SELECT
            platform,
            seller_nick,
            snick,
            tag_id,
            sum(tag_score_add_stat_count + tag_score_add_stat_md) AS tag_cnt
        FROM (
            -- 针对字段缺失的历史数据进行转换, 使其数据为0, 保证语法正确
            SELECT
                platform,
                seller_nick,
                snick,
                tag_score_add_stats_id AS tag_score_add_stats_id,
                -- 缺失历史数据直接为0, 对齐数组长度
                if(
                    length(tag_score_add_stats_count)!=length(tag_score_add_stats_id),
                    arrayResize([0],length(tag_score_add_stats_id),0),
                    tag_score_add_stats_count
                ) AS tag_score_add_stats_count,
                if(
                    length(tag_score_add_stats_md)!=length(tag_score_add_stats_id),
                    arrayResize([0],length(tag_score_add_stats_id),0),
                    tag_score_add_stats_md
                ) AS tag_score_add_stats_md
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) BETWEEN toYYYYMMDD(toDate('{{ day.start=week_ago }}')) AND toYYYYMMDD(toDate('{{ day.end=yesterday }}'))
            AND snick GLOBAL IN (
                -- 查询对应企业-平台的所有最新的子账号, 不论其是否绑定员工
                -- PS: 因为已经删除的子账号无法落入到最新的子账号分组中
                SELECT distinct snick
                FROM ods.xinghuan_employee_snick_all
                WHERE day = toYYYYMMDD(yesterday())
                AND platform = 'tb'
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            )
            -- 清除没有打标的数据, 减小计算量
            AND tag_score_add_stats_id!=[]
            -- 下拉框-店铺名
            AND (
                    '{{ seller_nicks }}'=''
                    OR
                    seller_nick IN splitByChar(',','{{ seller_nicks }}')
            )
            -- 下拉框-子账号
            AND (
                    '{{ snicks=null }}'=''
                    OR
                    snick IN splitByChar(',','{{ snicks=null }}')
            )
        ) AS transformed_dialog_info
        ARRAY JOIN
            tag_score_add_stats_id AS tag_id,
            tag_score_add_stats_count AS tag_score_add_stat_count,
            tag_score_add_stats_md AS tag_score_add_stat_md
        -- 清除空数据
        WHERE tag_score_add_stats_id!=[]
        GROUP BY platform, seller_nick, snick, tag_id
    ) AS human_check_tag_info
    GLOBAL LEFT JOIN (
        -- 人工质检标签维度表
        SELECT
            _id AS tag_id,
            name AS tag_name
        FROM ods.xdqc_tag_all
        WHERE day = toYYYYMMDD(yesterday())
        AND platform = 'tb'
        AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
    ) AS human_tag_info
    USING(tag_id)
    GROUP BY platform, seller_nick, snick
) AS man_check_info
GLOBAL LEFT JOIN (
    -- 获取最新版本的维度数据(T+1)
    SELECT
        snick, employee_name, superior_name, department_id, department_name
    FROM (
        SELECT snick, employee_name, superior_name, department_id
        FROM (
            -- 查询对应企业-平台的所有子账号及其部门ID, 不论其是否绑定员工
            SELECT snick, department_id, employee_id
            FROM ods.xinghuan_employee_snick_all
            WHERE day = toYYYYMMDD(yesterday())
            AND platform = 'tb'
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS snick_info
        GLOBAL LEFT JOIN (
            SELECT
                _id AS employee_id, username AS employee_name, superior_name
            FROM ods.xinghuan_employee_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS employee_info
        USING(employee_id)
    ) AS snick_info
    GLOBAL RIGHT JOIN (
        -- PS: 此处需要JOIN 3次来获取子账号分组的完整路径, 因为子账号分组树高为4
        -- parent_department_id全为空,则代表树层次遍历完毕
        SELECT
            level_1.parent_department_id AS parent_department_id,
            level_2_3_4.department_id AS department_id,
            if(
                level_1.department_id!='', 
                concat(level_1.department_name,'-',level_2_3_4.department_name),
                level_2_3_4.department_name
            ) AS department_name
        FROM (
            SELECT
                level_2.parent_department_id AS parent_department_id,
                level_3_4.department_id AS department_id,
                if(
                    level_2.department_id!='', 
                    concat(level_2.department_name,'-',level_3_4.department_name),
                    level_3_4.department_name
                ) AS department_name
            FROM (
                SELECT
                    level_3.parent_department_id AS parent_department_id,
                    level_4.department_id AS department_id,
                    if(
                        level_3.department_id!='', 
                        concat(level_3.department_name,'-',level_4.department_name),
                        level_4.department_name
                    ) AS department_name
                FROM (
                    SELECT 
                        _id AS department_id,
                        name AS department_name,
                        parent_id AS parent_department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                    AND (
                        parent_id GLOBAL IN (
                            SELECT DISTINCT
                                _id AS department_id
                            FROM ods.xinghuan_department_all
                            WHERE day = toYYYYMMDD(yesterday())
                            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                        ) -- 清除子账号父分组被删除, 而子分组依旧存在的脏数据
                        OR 
                        parent_id = '' -- 保留顶级分组
                    )
                ) AS level_4
                GLOBAL LEFT JOIN (
                    SELECT 
                        _id AS department_id,
                        name AS department_name,
                        parent_id AS parent_department_id
                    FROM ods.xinghuan_department_all
                    WHERE day = toYYYYMMDD(yesterday())
                    AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
                ) AS level_3
                ON level_4.parent_department_id = level_3.department_id
            ) AS level_3_4
            GLOBAL LEFT JOIN (
                SELECT 
                    _id AS department_id,
                    name AS department_name,
                    parent_id AS parent_department_id
                FROM ods.xinghuan_department_all
                WHERE day = toYYYYMMDD(yesterday())
                AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
            ) AS level_2
            ON level_3_4.parent_department_id = level_2.department_id
        ) AS level_2_3_4
        GLOBAL LEFT JOIN (
            SELECT 
                _id AS department_id,
                name AS department_name,
                parent_id AS parent_department_id
            FROM ods.xinghuan_department_all
            WHERE day = toYYYYMMDD(yesterday())
            AND company_id = '{{ company_id=5f747ba42c90fd0001254404 }}'
        ) AS level_1
        ON level_2_3_4.parent_department_id = level_1.department_id
    ) AS department_info
    USING (department_id)
) AS snick_department_map
USING(snick)
-- 下拉框-客服姓名
WHERE (
    '{{ usernames }}'=''
    OR
    employee_name IN splitByChar(',','{{ usernames }}')
)
GROUP BY platform, seller_nick, department_id, department_name, snick, employee_name, superior_name
HAVING department_id!='' -- 清除匹配不上历史分组的子账号
ORDER BY platform, seller_nick, department_name, snick, employee_name