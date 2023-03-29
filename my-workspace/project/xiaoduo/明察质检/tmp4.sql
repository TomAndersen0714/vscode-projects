SELECT
  group_id,
  groupArray(time)[1] as time,
  groupArray(value)
FROM ( 
  SELECT * FROM items ORDER BY time desc LIMIT 100 BY group_id
)
GROUP BY group_id format Null;