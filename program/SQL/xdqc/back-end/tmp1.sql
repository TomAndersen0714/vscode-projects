select tag_info.seller_nick,
    tag_info.`group`,
    'manual' as type,
    tag_info.snick,
    tag_info.tag_id as qc_id,
    all_tag_name_info.all_tag_name as qc_name,
    tag_info.qc_count as qc_count
from (
        select tag_id,
            seller_nick,
            `group`,
            snick,
            count(1) as qc_count
        from ods.xinghuan_dialog_tag_score_all
        where day = {{ ds_nodash }}
            and cal_op = 0
        group by tag_id,
            seller_nick,
            `group`,
            snick
    ) as tag_info
    left join (
        select norm_tag.tag_id,
            toString(
                concat(
                    if(
                        norm_tag.qc_norm_name = '',
                        '未设置一级标签',
                        norm_tag.qc_norm_name
                    ),
                    '/',
                    if(
                        sub_category.name = '',
                        '未设置二级标签',
                        sub_category.name
                    ),
                    '/',
                    norm_tag.tag_name
                )
            ) as all_tag_name
        from (
            select b._id as qc_norm_id,
                b.name as qc_norm_name,
                a._id as tag_id,
                a.name as tag_name,
                a.sub_category_id as sub_category_id
            from (
                    select _id,
                        category_id,
                        sub_category_id,
                        seller_nick,
                        qc_norm_id,
                        name
                    from ods.xdqc_tag_all
                    where day = {{ ds_nodash }}
                ) as a
                left join (
                    select _id,
                        name
                    from ods.xinghuan_qc_norm_all
                    where day = {{ ds_nodash }}
                        and status = 1
                ) as b
                on a.qc_norm_id = b._id
        ) as norm_tag
        left join (
            select _id,
                name
            from ods.xdqc_tag_sub_category_all
            where day = {{ ds_nodash }}
        ) as sub_category 
        on norm_tag.sub_category_id = sub_category._id
    ) as all_tag_name_info 
    on tag_info.tag_id = all_tag_name_info.tag_id
WHERE seller_nick IN [{{ shop_name }}]