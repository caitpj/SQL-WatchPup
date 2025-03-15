-- Extended analysis using file_schema.e07_status_trend_analysis.sql as a model
WITH status_trend_base AS (
    -- Directly referencing the previous model file
    SELECT * FROM (
        -- Assumes file_schema.e07_status_trend_analysis.sql exists and can be referenced
        SELECT 
            status,
            COUNT(*) as total_items,
            SUM(value) as total_value,
            AVG(value) as avg_value,
            GROUP_CONCAT(DISTINCT category) as related_categories
        FROM 
            file_schema.e07_status_trend_analysis
    ) base_data
)
SELECT 
    status,
    total_items,
    total_value,
    avg_value,
    related_categories,
    RANK() OVER (ORDER BY total_value DESC) as value_rank
FROM 
    status_trend_base
ORDER BY 
    total_value DESC;