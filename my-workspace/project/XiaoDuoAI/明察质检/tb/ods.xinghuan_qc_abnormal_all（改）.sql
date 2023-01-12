insert into ods.xinghuan_qc_abnormal_all
select
    { ds_nodash } as day,
    platform,
    seller_nick,
    _id,
    snick,
    abnormal_count,
    row_number
from (
        select
            platform,
            seller_nick,
            groupArray(_id) AS _id_array,
            groupArray(snick) as snick_array,
            groupArray(abnormal_count) as abnormal_count_array,
            arrayEnumerate(abnormal_count_array) AS row_number
        from (
            SELECT DISTINCT
                _id,
                platform,
                seller_nick,
                snick,
                score AS abnormal_count
            FROM dwd.xdqc_dialog_all
            WHERE toYYYYMMDD(begin_time) = { ds_nodash }
                and score > 0
            ORDER BY abnormal_count DESC
        )
        group by platform, seller_nick
    )
    ARRAY JOIN
        _id_array as _id,
        snick_array as snick,
        abnormal_count_array as abnormal_count,
        row_number