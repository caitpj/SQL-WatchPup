-- Advanced filtering based on previous status trend analysis
WITH status_trend AS (
    SELECT 
        status,
        COUNT(*) as total_items,
        SUM(value) as total_value,
        AVG(value) as avg_value
    FROM 
        source_table_3 s3
    JOIN 
        source_table_4 s4 ON s3.id = s4.source_3_id
    WHERE 
        s3.value > (SELECT AVG(value) FROM source_table_3)
    GROUP BY 
        status
)
SELECT 
    s3.name,
    s3.category,
    s3.value,
    s4.status,
    st.total_value as status_total_value
FROM 
    source_table_3 s3
JOIN 
    source_table_4 s4 ON s3.id = s4.source_3_id
JOIN 
    status_trend st ON s4.status = st.status
WHERE 
    s3.value > st.avg_value
    AND s4.status != 'Discontinued'
ORDER BY 
    s3.value DESC
LIMIT 10;