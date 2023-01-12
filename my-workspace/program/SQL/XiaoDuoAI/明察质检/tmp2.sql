
                WITH toYYYYMMDD(
                        date_add(
                            DAY,
                            toInt64('-1'),
                            addDays(toDate('2023-01-05'), -1)
                        )
                    ) AS compare_starts,
                    toYYYYMMDD(addDays(toDate('2023-01-05'), -1)) AS compare_ends
                SELECT platform,
                    corp_id,
                    shop_id,
                    bind_nick,
                    arrayMin(groupArrayArray(order_times)) as first_order_time
                FROM app_corp.corp_shop_nick_bind_order_stat_all
                WHERE day BETWEEN compare_starts AND compare_ends
                    AND corp_id = 'ww07ca2dcba6ae4bc9' -- AND platform='tb'
                    AND length(order_ids) > 0
                    AND bind_nick not like '%*%'
                GROUP BY platform,
                    corp_id,
                    shop_id,
                    bind_nick