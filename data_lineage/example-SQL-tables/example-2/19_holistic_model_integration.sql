-- Holistic integration of multiple previous model analyses
WITH advanced_filter_data AS (
    SELECT * FROM playground.08_advanced_filtering
),
category_performance_data AS (
    SELECT * FROM playground.06_category_performance
),
filtered_categories_data AS (
    SELECT * FROM playground.02_processed_source_data
),
value_aggregation_data AS (
    SELECT * FROM playground.03_value_aggregation
),
status_trend_data AS (
    SELECT * FROM playground.07_status_trend_analysis
)
SELECT 
    COALESCE(cp.category, fc.category, va.category, st.status) AS category,
    cp.high_value_count,
    cp.total_high_value,
    fc.item_count AS filtered_item_count,
    va.total_value AS aggregate_total_value,
    st.total_value AS status_total_value,
    COUNT(DISTINCT af.name) AS unique_advanced_filter_items
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
    COALESCE(cp.category, fc.category, va.category, st.status),
    cp.high_value_count,
    cp.total_high_value,
    fc.item_count,
    va.total_value,
    st.total_value
ORDER BY 
    COALESCE(cp.total_high_value, va.total_value, st.total_value) DESC
LIMIT 15;