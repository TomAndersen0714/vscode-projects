-- SELECT "店铺id",
--        "组别",
--        "昵称",
--        "姓名",
--        "咨询顾客数",
--        "有效打标顾客数",
--        "新增顾客打标率",
--        "日期",
--       --  arrayStringConcat(groupArray("标签组"),'$$') AS "标签组",
--        arrayStringConcat(groupArray("标签"),'$$') AS "标签",
--        arrayStringConcat(groupArray("数量"),'$$') AS "数量"
-- FROM
--   (SELECT '{{ shop_id_var=6143f37218f6b6000e173bc3 }}' AS "店铺id",
--           t1.`day` AS "日期",
--           t4.name AS "组别",
--           t1.snick AS "昵称",
--           t3.name AS "姓名",
--           t1.uv AS "咨询顾客数",
--           t1.effective_tag_uv AS "有效打标顾客数",
--           t1.effective_tag_rat AS "新增顾客打标率",
--           t2.group_name AS "标签组",
--           if ("标签组" = '产品需求',concat(t2.tag_name,'(自动)'),concat(t2.tag_name,'(人工)')) as "标签",
--           -- t2.tag_name AS "标签",
--           toString(t2.tag_cnt) AS "数量"
--    FROM
--      (SELECT t1.`day` AS `day`,
--              t1.snick,
--              t1.uv AS uv,
--              t2.effective_tag_uv AS effective_tag_uv,
--              concat(toString(round(effective_tag_uv*100/uv,2)),'%') AS effective_tag_rat
--       FROM
--         (SELECT `day`,
--                 snick,
--                 uniqExact(cnick) AS uv
--          FROM sxx_dws.snick_new_ask_detail_daily_all
--          WHERE  `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
--         AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
--            AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
--          GROUP BY `day`,
--                   snick
--          ORDER BY `day`,
--                   snick)t1
--       INNER JOIN
--         (SELECT t1.`day` AS `day`,
--                 t1.snick AS snick,
--                 uniqExact(t1.cnick) AS effective_tag_uv
--          FROM
--            (SELECT `day`,
--                    snick,
--                    shop_id,
--                    cnick
--             FROM sxx_dws.snick_new_ask_detail_daily_all
--             WHERE  `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
--         AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
--               AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}' )t1
--          JOIN
--            (SELECT `day`,
--                    cnick,
--                    shop_id,
--                    tag_id
--             FROM cdp_ods.tag_snapshot_all)t2 ON t1.`day`=t2.`day`
--          AND t1.shop_id=t2.shop_id
--          AND t1.cnick=t2.cnick
--          JOIN
--            (SELECT tag_id,
--                    shop_id,
--                    group_name
--             FROM cdp_dim.wt_tag_all
--             WHERE group_name IN ('意向等级',
--                                  '无效咨询'))t3 ON t2.tag_id=t3.tag_id
--          AND t2.shop_id=t3.shop_id
--          GROUP BY t1.`day`,
--                   t1.snick
--          ORDER BY t1.`day`,
--                   t1.snick)t2 USING (`day`,
--                                      snick)
--       ORDER BY t1.`day`,
--                snick)t1
--    LEFT JOIN
--      (SELECT t1.`day` AS `day`,
--              t1.snick AS snick,
--              t3.group_name AS group_name,
--              t3.tag_name AS tag_name,
--              uniqExact(t1.cnick) AS tag_cnt
--       FROM
--         (SELECT `day`,
--                 snick,
--                 shop_id,
--                 cnick
--          FROM sxx_dws.snick_new_ask_detail_daily_all
--          WHERE  `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
--         AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
--            AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}' )t1
--       JOIN
--         (SELECT `day`,
--                 shop_id,
--                 cnick,
--                 tag_id
--          FROM cdp_ods.tag_snapshot_all)t2 ON t1.`day`=t2.`day`
--       AND t1.shop_id=t2.shop_id
--       AND t1.cnick=t2.cnick
--       JOIN
--         (SELECT tag_id,
--                 shop_id,
--                 group_name,
--                 tag_name
--          FROM cdp_dim.wt_tag_all)t3 ON t2.tag_id=t3.tag_id
--       AND t2.shop_id=t3.shop_id
--       GROUP BY t1.`day`,
--                t1.snick,
--                t3.group_name,
--                t3.tag_name
--       ORDER BY t1.`day`,
--                t1.snick,
--                t3.group_name,
--                t3.tag_name)t2 ON t1.`day`=t2.`day`
--    AND t1.snick=t2.snick
--    LEFT JOIN
--      (SELECT snick,
--              argMax(name, `day`) AS name
--       FROM sxx_ods.snick_relation_daily_all
--       WHERE shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
--       GROUP BY snick)t3 ON t1.snick=t3.snick
--    LEFT JOIN
--      (SELECT splitByChar(':',subnick)[2] AS snick,
--              argMax(name, `day`) AS name
--       FROM ods.sub_user_group_all
--       WHERE shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
--       GROUP BY subnick)t4 ON t1.snick=t4.snick
--    ORDER BY t1.`day`,
--             t1.snick,
--             t2.group_name,
--             t2.tag_name)
-- GROUP BY "店铺id",
--          "组别",
--          "昵称",
--          "姓名",
--          "咨询顾客数",
--          "有效打标顾客数",
--          "新增顾客打标率",
--          "日期"
-- order by "日期" desc




SELECT "店铺id",
       "组别",
       "昵称",
       "姓名",
       "新增咨询人数",
       "有效打标顾客数",
       "新增顾客打标率",
       "日期",
      --  arrayStringConcat(groupArray("标签组"),'$$') AS "标签组",
       arrayStringConcat(groupArray("标签"),'$$') AS "标签",
       arrayStringConcat(groupArray("数量"),'$$') AS "数量"
FROM
  (SELECT '6143f37218f6b6000e173bc3' AS "店铺id",
       t1.`day` AS "日期",
       t4.name AS "组别",
       t1.snick AS "昵称",
       t3.name AS "姓名",
       t1.uv AS "新增咨询人数",
       t1.effective_tag_uv AS "有效打标顾客数",
       t1.effective_tag_rat AS "新增顾客打标率",
       t2.group_name AS "标签组",
       if ("标签组" = '产品需求',concat(t2.tag_name,'(自动)'),concat(t2.tag_name,'(人工)')) as "标签",
    --   t2.tag_name AS "标签",
       toString(t2.tag_cnt) AS "数量"
FROM
  (SELECT t1.`day` AS `day`,
          t1.snick,
          t1.uv AS uv,
          t2.effective_tag_uv AS effective_tag_uv,
          concat(toString(round(effective_tag_uv*100/uv,2)),'%') AS effective_tag_rat
   FROM
     (SELECT `day`,
             snick,
             uniqExact(cnick) AS uv
      FROM sxx_dws.snick_new_ask_detail_daily_all
      WHERE `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
        AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
        AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
      GROUP BY `day`,
               snick
      ORDER BY `day`,
               snick)t1
   INNER JOIN
     (SELECT t1.`day` AS `day`,
             t1.snick AS snick,
             uniqExact(t1.cnick) AS effective_tag_uv
      FROM
        (SELECT `day`,
                snick,
                shop_id,
                cnick
         FROM sxx_dws.snick_new_ask_detail_daily_all
         WHERE `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
           AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
           AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}' )t1
      JOIN
        (SELECT `day`,
                cnick,
                shop_id,
                tag_id
         FROM cdp_ods.tag_snapshot_all
         WHERE `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
           AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
           AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
           AND tag_id GLOBAL IN
             (SELECT DISTINCT tag_id
              FROM cdp_dim.wt_tag_all
              WHERE group_name IN('意向等级',
                                  '无效咨询')
                AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}' ) AND (`day`,cnick) GLOBAL IN
             (SELECT DISTINCT `day`,cnick
              FROM sxx_dws.snick_new_ask_detail_daily_all
              WHERE `day`>=cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
                AND `day`<=cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
                AND shop_id= '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'  ) )t2 ON t1.`day`=t2.`day`
      AND t1.shop_id=t2.shop_id
      AND t1.cnick=t2.cnick
      GROUP BY t1.`day`,
               t1.snick
      ORDER BY t1.`day`,
               t1.snick)t2 USING (`day`,
                                  snick)
   ORDER BY t1.`day`,
            snick)t1
LEFT JOIN
  (SELECT t1.`day` AS `day`,
          t1.snick AS snick,
          t3.group_name AS group_name,
          t3.tag_name AS tag_name,
          uniqExact(t1.cnick) AS tag_cnt
   FROM
     (SELECT `day`,
             snick,
             shop_id,
             cnick
      FROM sxx_dws.snick_new_ask_detail_daily_all
      WHERE `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
        AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
        AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}' )t1
   JOIN
     (SELECT `day`,
             shop_id,
             cnick,
             tag_id
      FROM cdp_ods.tag_snapshot_all
      WHERE `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
        AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
        AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
        AND (`day`,cnick) GLOBAL IN
          (SELECT DISTINCT `day`,cnick
           FROM sxx_dws.snick_new_ask_detail_daily_all
           WHERE `day` >= cast(replace('{{ tmp_day.start=7-day-ago }}', '-', '') AS int)
             AND `day` <= cast(replace('{{ tmp_day.end=1-day-ago }}', '-', '') AS int)
             AND shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}') )t2 ON t1.`day`=t2.`day`
   AND t1.shop_id=t2.shop_id
   AND t1.cnick=t2.cnick
   JOIN
     (SELECT tag_id,
             shop_id,
             group_name,
             tag_name
      FROM cdp_dim.wt_tag_all
      WHERE shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}')t3 ON t2.tag_id=t3.tag_id
   AND t2.shop_id=t3.shop_id
   GROUP BY t1.`day`,
            t1.snick,
            t3.group_name,
            t3.tag_name
   ORDER BY t1.`day`,
            t1.snick,
            t3.group_name,
            t3.tag_name)t2 ON t1.`day`=t2.`day`
AND t1.snick=t2.snick
INNER JOIN
  (SELECT snick,
          argMax(name, `day`) AS name
   FROM sxx_ods.snick_relation_daily_all
   WHERE shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
   GROUP BY snick)t3 ON t1.snick=t3.snick
INNER JOIN
  (SELECT snick,name1 as name
  FROM
    (SELECT splitByChar(':',subnick)[2] AS snick,
          argMax(name, `day`) AS name1
   FROM ods.sub_user_group_all
   WHERE shop_id = '{{ shop_id_var=6143f37218f6b6000e173bc3 }}'
   and name='售前全员'
   GROUP BY subnick)
  --  WHERE name='售前全员'
   )t4 ON t1.snick=t4.snick
ORDER BY t1.`day`,
         t1.snick,
         t2.group_name,
         t2.tag_name)
GROUP BY "店铺id",
         "组别",
         "昵称",
         "姓名",
         "新增咨询人数",
         "有效打标顾客数",
         "新增顾客打标率",
         "日期"
order by "日期" desc