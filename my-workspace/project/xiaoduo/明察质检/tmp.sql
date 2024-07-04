set hive.optimize.cte.materialize.threshold=1

with a as (
  select 1
),
b as (
  select 2
)
select *
from a
  join b;