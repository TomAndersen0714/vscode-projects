-- 
select *
from table t1
where 1 > (
    select count(*)
    from table t2
    where t1.departmeny_id = t2.department_id
    and t1.salary > t2.salary
)

-- 
select employee_id
from table t1
left join (
    select department_id,
        max(salary) as maxSal
    from table
    group by department_id
) t2 
on t1.department_id = t2.department_id
where t1.salary = t2.maxSal;

-- rank
select employee_id
from (
    select employee_id,
        rank() over (
            partition by department_id
            order by salary desc
        ) as r
    from table
) t
where r = 1