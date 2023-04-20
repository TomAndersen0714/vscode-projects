insert into dim.question_b
select `_id`,
    `qid`,
    `question`,
    `subcategory_id`,
    `tags`,
    `answers`,
    `is_transfer`,
    `is_dynamicable`,
    `is_dynamic`,
    `is_editable`,
    `auto_send_in_hybrid_mode`,
    `auto_send_in_auto_mode`,
    `replies`,
    `create_time`,
    `update_time`
from (
        select subcategory_id
        from (
                select _id
                from dim.category_all
                where platform = 7
            ) as a
            left join dim.category_subcategory_all as b on a._id = b.category_id
        where subcategory_id <> ''
        group by subcategory_id
    ) as d
    left join tmp.question_b_all as c on d.subcategory_id = c.subcategory_id