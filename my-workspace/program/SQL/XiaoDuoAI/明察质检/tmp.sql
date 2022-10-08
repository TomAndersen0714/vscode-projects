SELECT COUNT(1)
FROM (
    SELECT *
    FROM update_test_limit_by
    ORDER BY update_time
    LIMIT 1 BY user_id
)