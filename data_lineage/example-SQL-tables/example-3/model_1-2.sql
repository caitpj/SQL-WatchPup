SELECT COUNT(*) 
FROM {schema}.model_1-1
cross JOIN (
    values ('a', 'b')
) c(cols)
cross join lateral (
    select * from {schema}.model_1-1
) a