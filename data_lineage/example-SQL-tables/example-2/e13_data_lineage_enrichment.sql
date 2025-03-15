-- Extended analysis using file_schema.e06_category_performance.sql as a model
WITH category_performance_base AS (
    -- Directly referencing the previous model file
    SELECT * FROM (
        -- Assumes file_schema.e06_category_performance.sql exists and can be referenced
        SELECT 
            category,
            COUNT(*) as high_value_count,
            AVG(value) as avg_high_value,
            SUM(value) as total_high_value,
            GROUP_CONCAT(DISTINCT status) as unique_statuses
        FROM 
            file_schema.e06_category_performance
    ) base_data
)
SELECT 
    category,
    high_value_count,
    total_high_value,
    avg_high_value,
    unique_statuses,
    DENSE_RANK() OVER (ORDER BY total_high_value DESC) as performance_rank
FROM 
    category_performance_base
ORDER BY 
    total_high_value DESC;