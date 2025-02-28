-- Identify high-value items based on previous status distribution
WITH status_distribution AS (
    SELECT 
        s4.status, 
        COUNT(*) as status_count,
        SUM(s3.value) as total_status_value
    FROM 
        source_table_3 s3
    JOIN 
        source_table_4 s4 ON s3.id = s4.source_3_id
    WHERE 
        s3.value > 500
    GROUP BY 
        s4.status
)
SELECT 
    s3.name,
    s3.category,
    s3.value,
    s4.status,
    sd.total_status_value
FROM 
    source_table_3 s3
JOIN 
    source_table_4 s4 ON s3.id = s4.source_3_id
JOIN 
    status_distribution sd ON s4.status = sd.status
WHERE 
    s3.value > (SELECT AVG(value) * 1.5 FROM source_table_3);