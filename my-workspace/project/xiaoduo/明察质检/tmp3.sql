            SELECT
                '{platform}' AS `platform`,
                '{shop_id}' AS `shop_id`,
                '{shop_name}' AS `shop_name`,
                replaceOne(splitByChar(':', user_nick)[1], 'cntaobao', '') AS `seller_nick`,
                replaceOne(user_nick, 'cntaobao', '') AS `snick`,
                replaceOne(eval_recer,'cntaobao','') AS `cnick`,
                '' AS `real_buyer_nick`,
                '' AS `open_uid`,
                `user_nick`,
                `eval_code`,
                `eval_recer`,
                `eval_sender`,
                `eval_time`,
                `send_time`,
                `source`,
                `day`
            FROM ods.kefu_eval_detail_all
            WHERE day = {ds_nodash}
            AND user_nick LIKE 'cnjd{seller_nick}%'