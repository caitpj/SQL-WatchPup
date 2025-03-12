-- Initial query using s.source_table_3 and s.source_table_4
SELECT 
    s3.id, 
    s3.name, 
    s3.category, 
    s3.value, 
    s4.status, 
    s4.additional_info
FROM 
    s.source_table_3 s3
JOIN 
    s.source_table_4 s4 ON s3.id = s4.source_e3_id
WHERE 
    s3.value > 500;