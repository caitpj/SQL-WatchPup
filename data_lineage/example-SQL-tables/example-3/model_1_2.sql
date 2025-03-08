SELECT COUNT(*) 
FROM file_schema.model_1
cross JOIN (
    values ('a', 'b')
) c(cols)
cross join lateral (
    select * from file_schema.model_1_1
) a