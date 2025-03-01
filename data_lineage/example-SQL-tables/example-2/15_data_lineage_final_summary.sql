-- Extended analysis using playground.02_processed_source_data.sql as a model
WITH filtered_categories_base AS (
    -- Directly referencing the previous model file
    SELECT * FROM (
        -- Assumes playground.02_processed_source_data.sql exists and can be referenced
        SELECT 
            name, 
            category, 
            value, 
            status
        FROM 
            playground.02_processed_source_data
    ) base_data
)
SELECT 
    category,
    COUNT(*) as category_count,
    MIN(value) as min_value,
    MAX(value) as max_value,
    AVG(value) as avg_value,
    GROUP_CONCAT(DISTINCT status) as unique_statuses
FROM 
    filtered_categories_base
GROUP BY 
    category
ORDER BY 
    avg_value DESC;