INSERT INTO {sink_table}
SELECT
    company_id,
    group_id,
    group_name,
    group_level,
    parent_group_ids[1] AS parent_group_id,
    parent_group_names[1] AS parent_group_name,
    parent_group_ids[-1] AS first_group_id,
    parent_group_names[-1] AS first_group_name,
    parent_group_ids[-2] AS second_group_id,
    parent_group_names[-2] AS second_group_name,
    parent_group_ids[-3] AS third_group_id,
    parent_group_names[-3] AS third_group_name,
    parent_group_ids[-4] AS fourth_group_id,
    parent_group_names[-4] AS fourth_group_name,
    create_time,
    update_time
FROM (
    SELECT
        child.company_id AS company_id,
        child.group_id AS group_id,
        child.group_name AS group_name,
        child.group_level AS group_level,
        parent.group_id AS next_parent_group_id,
        arrayPushBack(parent_group_ids, child.next_parent_group_id) AS parent_group_ids,
        arrayPushBack(parent_group_names, parent.group_name) AS parent_group_names,
        child.create_time AS create_time,
        child.update_time AS update_time
    FROM (
        SELECT
            child.company_id AS company_id,
            child.group_id AS group_id,
            child.group_name AS group_name,
            child.group_level AS group_level,
            parent.group_id AS next_parent_group_id,
            arrayPushBack(parent_group_ids, child.next_parent_group_id) AS parent_group_ids,
            arrayPushBack(parent_group_names, parent.group_name) AS parent_group_names,
            child.create_time AS create_time,
            child.update_time AS update_time
        FROM (
            SELECT
                child.company_id AS company_id,
                child.group_id AS group_id,
                child.group_name AS group_name,
                child.group_level AS group_level,
                parent.group_id AS next_parent_group_id,
                arrayPushBack(parent_group_ids, child.next_parent_group_id) AS parent_group_ids,
                arrayPushBack(parent_group_names, parent.group_name) AS parent_group_names,
                child.create_time AS create_time,
                child.update_time AS update_time
            FROM (
                SELECT
                    company_id,
                    _id AS group_id,
                    name AS group_name,
                    level AS group_level,
                    parent_id AS next_parent_group_id,
                    [] AS parent_group_ids,
                    [] AS parent_group_names,
                    create_time,
                    update_time
                FROM dim.voc_question_b_group_all
            ) AS child
            GLOBAL LEFT JOIN (
                SELECT
                    _id AS group_id,
                    name AS group_name,
                    parent_id
                FROM dim.voc_question_b_group_all
            ) AS parent
            ON child.next_parent_group_id = parent.group_id
        ) AS child
        GLOBAL LEFT JOIN (
            SELECT
                _id AS group_id,
                name AS group_name,
                parent_id
            FROM dim.voc_question_b_group_all
        ) AS parent
        ON child.next_parent_group_id = parent.group_id
    ) AS child
    GLOBAL LEFT JOIN (
        SELECT
            _id AS group_id,
            name AS group_name,
            parent_id
        FROM dim.voc_question_b_group_all
    ) AS parent
    ON child.next_parent_group_id = parent.group_id
)