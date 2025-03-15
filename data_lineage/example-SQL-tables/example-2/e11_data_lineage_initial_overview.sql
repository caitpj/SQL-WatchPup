-- Extended analysis using file_schema.e08_advanced_filtering.sql as a model
WITH advanced_filter_base AS (
    -- Directly referencing the previous model file
    SELECT * FROM (
        -- Assumes file_schema.e08_advanced_filtering.sql exists and can be referenced
        SELECT 
            name,
            category,
            value,
            status,
            status_total_value
        FROM 
            file_schema.e08_advanced_filtering
    ) base_data
)
SELECT 
    category,
    COUNT(*) as category_count,
    SUM(value) as total_category_value,
    AVG(value) as avg_category_value,
    GROUP_CONCAT(DISTINCT status) as unique_statuses
FROM 
    advanced_filter_base
GROUP BY 
    category
ORDER BY 
    total_category_value DESC;