

with cte_x as (
    select a,b 
    from /*fake.table*/ raw_source.x_base
)
    
, y_base as (
    select a,b
    -- from fake_table
    from cte_x
    inner join raw_source.y_base 
        using(id) 
)
select *
from y_base
