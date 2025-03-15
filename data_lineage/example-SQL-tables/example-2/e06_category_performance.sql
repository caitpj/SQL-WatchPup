-- Analyze category performance from high-value items
WITH high_value_items AS (
    SELECT 
        s3.name,
        s3.category,
        s3.value,
        s4.status
    FROM 
        file_schema.e01_initial_source_tables s3
    JOIN 
        s.source_table_4 s4 ON s3.id = s4.source_e3_id
    WHERE 
        s3.value > (SELECT AVG(value) * 1.5 FROM s.source_table_3)
)
SELECT 
    category,
    COUNT(*) as high_value_count,
    AVG(value) as avg_high_value,
    SUM(value) as total_high_value,
    GROUP_CONCAT(DISTINCT status) as unique_statuses
FROM 
    high_value_items
GROUP BY 
    category
ORDER BY 
    total_high_value DESC;