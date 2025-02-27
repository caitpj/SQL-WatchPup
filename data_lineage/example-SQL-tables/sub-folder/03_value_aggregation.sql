-- Aggregate values from previous filtered categories
WITH filtered_categories AS (
    SELECT 
        name, 
        category, 
        value, 
        status
    FROM 
        source_table_3 s3
    JOIN 
        source_table_4 s4 ON s3.id = s4.source_3_id
    WHERE 
        s3.value > 500 AND category IN ('Electronics', 'Home Goods')
)
SELECT 
    category, 
    COUNT(*) as item_count,
    SUM(value) as total_value,
    AVG(value) as avg_value
FROM 
    filtered_categories
GROUP BY 
    category;