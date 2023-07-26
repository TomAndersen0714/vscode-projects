drop table X

create table X(A Array(String)) engine = Memory;

insert into X select arrayMap(x->toString (x) , range(1, 1001)) from numbers(50);

select arrayFilter(x->x='666', A) from X format Null;
-- Peak memory usage (for query): 0.00 B.

select arrayFilter((x, y)->A[y]='666', A, arrayEnumerate(A)) from X format Null;
-- Peak memory usage (for query): 1.00 GiB.

-- Array function 的 lambda expression 中取消Array引用, 有效
select arrayFilter((v, k)-> k = '666', A, A) from X format Null;
Peak memory usage (for query): 0.00 B.

-- 设置 max_block_size, 无效
select arrayFilter((x, y)->A[y]='666', A, arrayEnumerate(A)) from X SETTINGS max_block_size = 1 format Null;
Peak memory usage (for query): 1.00 GiB.


-- 设置 max_threads, 无效
select arrayFilter((x, y)->A[y]='666', A, arrayEnumerate(A)) from X SETTINGS max_threads=1 format Null;


-- 设置 max_block_size 和 max_threads, 无效
select arrayFilter((x, y)->A[y]='666', A, arrayEnumerate(A)) from X SETTINGS max_block_size = 1, max_threads=1 format Null;
Peak memory usage (for query): 1.00 GiB.


-- 嵌套子查询, 无效
select arrayFilter((x, y)->A[y]='666', A, keys) from (select A, arrayEnumerate(A) as keys from X) format Null;
Peak memory usage (for query): 1.00 GiB.


-- 更换数组生成函数为range, 无效
select arrayFilter((x, y)->A[y]='666', A, keys) from (select A, range(length(A)) as keys from X) format Null;
Peak memory usage (for query): 1.00 GiB.

-- 嵌套子查询, Array function 的 lambda expression 中取消Array引用, 有效
select arrayFilter((v, k)-> k = 666, A, keys) from (select A, arrayEnumerate(A) as keys from X) format Null;
Peak memory usage (for query): 0.00 B.