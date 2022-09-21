-- 方太京东转接记录
ALTER TABLE ft_dwd.transfer_msg_local ON CLUSTER cluster_3s_2r DROP PARTITION {{ds_nodash}};

SELECT sleep(3);

INSERT INTO ft_dwd.transfer_msg_all
SELECT
    lower(hex(MD5(concat(shop_id, from_snick, to_snick, cnick, real_buyer_nick, create_time)))) AS id,
    day,
    platform,
    '{{shop_id}}' AS shop_id,
    '{{shop_name}}' AS shop_name,
    from_spin AS from_snick,
    to_spin AS to_snick,
    buyer_one_id AS cnick,
    pin AS real_buyer_nick,
    create_time
FROM ods.transfer_msg_all
WHERE day = {{ds_nodash}}
AND platform = 'jd'
AND plat_user_id = '{{shop_name}}';

-- 等待数据写入
SELECT sleep(3);