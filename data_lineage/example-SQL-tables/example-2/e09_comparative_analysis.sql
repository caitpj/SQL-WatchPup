-- Comparative analysis from advanced filtering
WITH advanced_filter AS (
    SELECT 
        category,
        status,
        COUNT(*) as item_count,
        SUM(value) as total_value,
        AVG(value) as avg_value
    FROM 
        s.source_table_3 s3
    JOIN 
        s.source_table_4 s4 ON s3.id = s4.source_e3_id
    WHERE 
        s3.value > (SELECT AVG(value) FROM s.source_table_3)
        AND s4.status != 'Discontinued'
    GROUP BY 
        category, status
)
SELECT 
    category,
    GROUP_CONCAT(DISTINCT status) as statuses,
    SUM(item_count) as total_items,
    SUM(total_value) as cumulative_value,
    AVG(avg_value) as average_category_value
FROM 
    advanced_filter
GROUP BY 
    category
HAVING 
    total_items > 1
ORDER BY 
    cumulative_value DESC;