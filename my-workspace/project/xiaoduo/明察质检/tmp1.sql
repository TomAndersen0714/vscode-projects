表t1的数据
id
1
2
3
表t2的数据
id
1
2
2


-- 题目1 执行结果是什么样的
 select t1.id, t2.id
from t1 left join t2 
on t1.id = t2.id and t2.id <> 2;

t1.id, t2.id:
1, 1
2, null
3, null


-- 题目2 执行结果是什么样的
select t1.id, t2.id
from t1 left join t2 
on t1.id = t2.id
where t2.id <> 2；

t1.id, t2.id:
1, 1