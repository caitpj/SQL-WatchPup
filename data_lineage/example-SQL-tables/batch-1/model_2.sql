select * from playground.model_1
left join sand.source_table_b using(id)
-- left join sand.source_table_c using(id) -- commented out