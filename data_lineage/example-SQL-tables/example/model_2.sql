with cte_1 as (
    select * from model_1
    where company = 'XXX'
)
select 
    reporting_date,
    sum(revenue) as daily_revenue
from cte_1
group by reporting_date