-- Analyze status distribution from previous aggregations
WITH value_aggregation AS (
    SELECT 
        category, 
        COUNT(*) as item_count,
        SUM(value) as total_value,
        AVG(value) as avg_value
    FROM 
        source_table_3 s3
    JOIN 
        source_table_4 s4 ON s3.id = s4.source_3_id
    WHERE 
        s3.value > 500 AND category IN ('Electronics', 'Home Goods')
    GROUP BY 
        category
)
SELECT 
    s4.status, 
    COUNT(*) as status_count,
    SUM(s3.value) as total_status_value
FROM 
    source_table_3 s3
JOIN 
    source_table_4 s4 ON s3.id = s4.source_3_id
JOIN 
    value_aggregation va ON s3.category = va.category
GROUP BY 
    s4.status;