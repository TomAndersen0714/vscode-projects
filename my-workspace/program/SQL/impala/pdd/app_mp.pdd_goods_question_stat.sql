insert overwrite app_mp.pdd_goods_question_stat partition (day={{ ds_nodash }})
WITH  t2 AS
  (SELECT platform,
          shop_oid as shop_id,
          category,
          xd_shop_nick,
          plat_goods_id,
          count(1) AS pv,
          count(DISTINCT cnick) AS cuv
  FROM dwd.pdd_xdrs_logs
  WHERE DAY = {{ ds_nodash }}
     AND act = 'recv_msg'
     AND plat_goods_id != ''
  GROUP BY platform,
            shop_oid,
            category,
            xd_shop_nick,
            plat_goods_id ),
     t3 AS
  (SELECT DISTINCT shop_name ,object_id as shop_id
   FROM xd_data.shop
   WHERE platform = 'pdd'
     AND DAY = {{ ds_nodash }}),
     t4 AS
  (SELECT *
   FROM xd_metadata.category),
     t5 AS
  (SELECT DISTINCT plat_goods_id,
                   plat_goods_name
   FROM ods.pdd_xdmp_goods),
     t6 AS
  (SELECT xd_shop_nick as shop_name ,
          plat_goods_id,
          cuv,
          category
   FROM t2
   left JOIN t3 ON t2.shop_id = t3.shop_id),
     t7 AS
  (SELECT shop_name,
          plat_goods_id,
          cuv,
          name
   FROM t6
   JOIN t4 ON t6.category = t4.nlu_code)
SELECT name,
       split_part(shop_name,'cnpdd',2)  as shop_name,
       t7.plat_goods_id,
       plat_goods_name,
       cuv
FROM t7
JOIN t5 ON t7.plat_goods_id = t5.plat_goods_id
WHERE t7.plat_goods_id IS NOT NULL
  AND t7.plat_goods_id != '' ;
