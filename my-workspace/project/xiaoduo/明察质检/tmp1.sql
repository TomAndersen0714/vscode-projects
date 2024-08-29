SELECT
    t1.key
FROM (
    SELECT
        -- 你好, 这里是你的代码
        concat(floor(rand()*10), key) AS new_key
    FROM t1
) t1
INNER JOIN (
    SELECT concat(prefix, key) as new_key
    FROM t2
    LATERAL VIEW explode(array(1,2,3,4,5,6,7,8,9,10)) t AS prefix
) t2
on t1.new_key = t2.new_key;

