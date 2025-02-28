

with cte_x as (select a,b from /*{schema}*/ bags.x_base) , y_base as (select a,b,c from /*{schema}*/ cte_x inner join bage.y_base  using(id) )
select *
from y_base
