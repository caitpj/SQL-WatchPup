-- Analyze status trends from category performance
WITH category_performance AS (
    SELECT 
        category,
        COUNT(*) as high_value_count,
        AVG(value) as avg_high_value,
        SUM(value) as total_high_value,
        GROUP_CONCAT(DISTINCT status) as unique_statuses
    FROM 
        source_table_3 s3
    JOIN 
        source_table_4 s4 ON s3.id = s4.source_3_id
    WHERE 
        s3.value > (SELECT AVG(value) * 1.5 FROM source_table_3)
    GROUP BY 
        category
)
SELECT 
    s4.status,
    COUNT(*) as total_items,
    SUM(s3.value) as total_value,
    AVG(s3.value) as avg_value,
    GROUP_CONCAT(DISTINCT cp.category) as related_categories
FROM 
    source_table_3 s3
JOIN 
    source_table_4 s4 ON s3.id = s4.source_3_id
JOIN 
    category_performance cp ON s3.category = cp.category
GROUP BY 
    s4.status
ORDER BY 
    total_value DESC;