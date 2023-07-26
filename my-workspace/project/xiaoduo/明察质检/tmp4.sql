drop table test.X

create table test.X(A Array(String)) engine = Memory;

insert into test.X select arrayMap(x->toString (x) , range(1000)) from numbers(20);

select arrayFilter(x->x='777', A) from test.X format Null;
Peak memory usage (for query): 0.00 B


select arrayFilter(x->A[x]='777', arrayEnumerate(A)) from test.X format Null;
Peak memory usage (for query): 512.63 MiB


select arrayFilter(x->A[x]='777', keys) from (select A, arrayEnumerate(A) as keys from test.X)
format Null;
Peak memory usage (for query): 512.66 MiB

select arrayFilter((v, k)-> k = '777', A, A) from test.X format Null;
Peak memory usage (for query): 14.20 MiB.