-- Initial query using source_table_3 and source_table_4
SELECT 
    s3.id, 
    s3.name, 
    s3.category, 
    s3.value, 
    s4.status, 
    s4.additional_info
FROM 
    source_table_3 s3
JOIN 
    source_table_4 s4 ON s3.id = s4.source_3_id
WHERE 
    s3.value > 500;