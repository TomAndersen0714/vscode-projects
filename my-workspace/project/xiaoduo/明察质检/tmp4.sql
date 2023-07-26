drop table test.X

create table test.X(A Array(String)) engine = Memory;

insert into test.X select arrayMap(x->toString (x) , range(1, 1001)) from numbers(50);


select arrayFilter(x->x='777', A) from test.X format Null;
Peak memory usage (for query): 0.00 B.

select arrayFilter((x, y)->A[y]='777', A, arrayEnumerate(A)) from test.X format Null;
Peak memory usage (for query): 1.00 GiB.


-- 设置 max_block_size, 无效
select arrayFilter((x, y)->A[y]='777', A, arrayEnumerate(A)) from test.X SETTINGS max_block_size = 1 format Null;
Peak memory usage (for query): 1.00 GiB.


-- 设置 max_threads, 无效
select arrayFilter((x, y)->A[y]='777', A, arrayEnumerate(A)) from test.X SETTINGS max_threads=1 format Null;


-- 设置 max_block_size 和 max_threads, 无效
select arrayFilter((x, y)->A[y]='777', A, arrayEnumerate(A)) from test.X SETTINGS max_block_size = 1, max_threads=1 format Null;
Peak memory usage (for query): 1.00 GiB.


-- 嵌套子查询无效
select arrayFilter((x, y)->A[y]='777', A, keys) from (select A, arrayEnumerate(A) as keys from test.X) format Null;
Peak memory usage (for query): 1.00 GiB.


-- 更换数组生成函数为range, 无效
select arrayFilter((x, y)->A[y]='777', A, keys) from (select A, range(length(A)) as keys from test.X) format Null;
Peak memory usage (for query): 1.00 GiB.


select arrayFilter((x, y)->A[y]='777', A, keys) from (select A, arrayEnumerate(A) as keys from test.X) format Null
settings max_block_size = 1;
Peak memory usage (for query): 1.00 GiB.


-- 
select arrayFilter((v, k)-> k = '777', A, A) from test.X format Null;
Peak memory usage (for query): 0.00 B.


-- lambda 中取消Array引用, 有效
select arrayFilter((v, k)-> k = 777, A, keys) from (select A, arrayEnumerate(A) as keys from test.X) format Null;
Peak memory usage (for query): 0.00 B.