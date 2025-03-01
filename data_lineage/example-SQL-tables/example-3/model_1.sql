

with cte_x as (
    select a,b 
    from /*{schema}*/ raw_source.x_base
)
    
, y_base as (
    select a,b
    from /*{schema}*/ cte_x
    inner join raw_source.y_base 
        using(id) 
)
select *
from y_base
