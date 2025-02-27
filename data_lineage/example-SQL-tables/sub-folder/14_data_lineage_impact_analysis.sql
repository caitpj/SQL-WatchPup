-- Extended analysis using 03_value_aggregation.sql as a model
WITH value_aggregation_base AS (
    -- Directly referencing the previous model file
    SELECT * FROM (
        -- Assumes 03_value_aggregation.sql exists and can be referenced
        SELECT 
            category, 
            COUNT(*) as item_count,
            SUM(value) as total_value,
            AVG(value) as avg_value
        FROM 
            03_value_aggregation
    ) base_data
)
SELECT 
    category,
    item_count,
    total_value,
    avg_value,
    PERCENT_RANK() OVER (ORDER BY total_value) as value_percentile
FROM 
    value_aggregation_base
ORDER BY 
    total_value DESC;