INSERT INTO app_corp.corp_customer_stat_all (
                day,
                corp_id,
                total,
                out_sync,
                relation_counts,
                bind_counts,
                add_num,
                dec_num,
                adds,
                decs
        )
WITH 20221026 as today
SELECT today AS day,
        corp_id,
        countIf(DISTINCT external_user_id, sync_status = 0) AS total,
        countIf(DISTINCT external_user_id, sync_status != 0) as out_sync,
        CAST(
                (
                        [1,2,3,4,5],
                        [ countIf(DISTINCT external_user_id, sync_status = 0 AND relation = 1), countIf(DISTINCT external_user_id, sync_status = 0 AND relation = 2), countIf(DISTINCT external_user_id, sync_status = 0 AND relation = 3), countIf(DISTINCT external_user_id, sync_status = 0 AND relation = 4), countIf(DISTINCT external_user_id, sync_status = 0 AND relation = 5)]
                ),
                'Map(Int,Int64)'
        ) AS relation_counts,
        CAST(
                (
                        ['all','tb','jd','yz','dy'],
                        [ countIf(DISTINCT external_user_id, sync_status = 0 AND (tb_nick != '' OR jd_nick != '' OR yz_nick != '' OR dy_nick != '') ), countIf(DISTINCT external_user_id, sync_status = 0 AND tb_nick != ''), countIf(DISTINCT external_user_id, sync_status = 0 AND jd_nick != ''), countIf(DISTINCT external_user_id, sync_status = 0 AND yz_nick != ''), countIf(DISTINCT external_user_id, sync_status = 0 AND dy_nick != '')]
                ),
                'Map(String,Int64)'
        ) AS bind_counts,
        length(adds) AS add_num,
        length(decs) AS dec_num,
        groupUniqArrayIf(external_user_id, add_day = today) AS adds,
        groupUniqArrayIf(external_user_id, dec_day = today) AS decs
FROM (
                SELECT corp_id,
                        external_user_id,
                        argMax(wx_nick, pushed_at) as wx_nick,
                        argMax(add_day, pushed_at) as add_day,
                        argMax(dec_day, pushed_at) as dec_day,
                        argMax(tb_nick, pushed_at) as tb_nick,
                        argMax(jd_nick, pushed_at) as jd_nick,
                        argMax(yz_nick, pushed_at) as yz_nick,
                        argMax(dy_nick, pushed_at) as dy_nick,
                        argMax(relation, pushed_at) as relation,
                        argMax(sync_status, pushed_at) as sync_status
                FROM ods.corp_customer_all
                WHERE part_id = 5
                GROUP BY corp_id,
                        external_user_id
        )
WHERE add_day <= today
GROUP BY corp_id