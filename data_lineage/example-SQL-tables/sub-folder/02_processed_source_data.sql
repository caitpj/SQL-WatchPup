-- Query based on previous result, filtering specific categories
WITH initial_data AS (
    SELECT 
        id, 
        name, 
        category, 
        value, 
        status, 
        additional_info
    FROM 
        source_table_3 s3
    JOIN 
        source_table_4 s4 ON s3.id = s4.source_3_id
    WHERE 
        s3.value > 500
)
SELECT 
    name, 
    category, 
    value, 
    status
FROM 
    initial_data
WHERE 
    category IN ('Electronics', 'Home Goods');