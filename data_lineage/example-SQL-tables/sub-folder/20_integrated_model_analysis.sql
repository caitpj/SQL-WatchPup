-- Integrated analysis across multiple previous models
WITH advanced_filter_data AS (
    SELECT * FROM 08_advanced_filtering
),
category_performance_data AS (
    SELECT * FROM 06_category_performance
),
filtered_categories_data AS (
    SELECT * FROM 02_processed_source_data
),
value_aggregation_data AS (
    SELECT * FROM 03_value_aggregation
),
status_trend_data AS (
    SELECT * FROM 07_status_trend_analysis
)
SELECT 
    COALESCE(cp.category, fc.category, va.category, st.status) AS category,
    AVG(
        COALESCE(cp.total_high_value, va.total_value, 0)
    ) AS normalized_total_value,
    COUNT(DISTINCT 
        CASE 
            WHEN af.name IS NOT NULL THEN af.name 
            WHEN fc.name IS NOT NULL THEN fc.name 
        END
    ) AS total_unique_items,
    MAX(
        COALESCE(cp.high_value_count, fc.item_count, va.item_count, st.total_items)
    ) AS max_item_count,
    SUM(
        COALESCE(st.total_value, va.total_value, cp.total_high_value, 0)
    ) AS cumulative_value
FROM 
    category_performance_data cp
FULL OUTER JOIN 
    filtered_categories_data fc ON cp.category = fc.category
FULL OUTER JOIN 
    value_aggregation_data va ON COALESCE(cp.category, fc.category) = va.category
FULL OUTER JOIN 
    status_trend_data st ON COALESCE(cp.category, fc.category, va.category) = st.status
LEFT JOIN 
    advanced_filter_data af ON COALESCE(cp.category, fc.category, va.category) = af.category
GROUP BY 
    COALESCE(cp.category, fc.category, va.category, st.status)
ORDER BY 
    normalized_total_value DESC
LIMIT 15;