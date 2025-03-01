-- Final summary query based on previous comparative analysis
WITH comparative_analysis AS (
    SELECT 
        category,
        GROUP_CONCAT(DISTINCT status) as statuses,
        SUM(
            (SELECT COUNT(*) 
             FROM source_table_3 s3 
             JOIN source_table_4 s4 ON s3.id = s4.source_3_id 
             WHERE s3.category = af.category)
        ) as total_items,
        SUM(
            (SELECT SUM(value) 
             FROM source_table_3 s3 
             JOIN source_table_4 s4 ON s3.id = s4.source_3_id 
             WHERE s3.category = af.category)
        ) as cumulative_value
    FROM 
        (SELECT 
            category,
            status,
            COUNT(*) as item_count,
            SUM(value) as total_value,
            AVG(value) as avg_value
        FROM 
            source_table_3 s3
        JOIN 
            source_table_4 s4 ON s3.id = s4.source_3_id
        WHERE 
            s3.value > (SELECT AVG(value) FROM source_table_3)
            AND s4.status != 'Discontinued'
        GROUP BY 
            category, status
        ) af
    GROUP BY 
        category
    HAVING 
        total_items > 1
)
SELECT 
    category,
    statuses,
    total_items,
    cumulative_value,
    RANK() OVER (ORDER BY cumulative_value DESC) as value_rank
FROM 
    comparative_analysis
ORDER BY 
    cumulative_value DESC;