-- Deep cross-model insights and correlations
WITH advanced_filter_data AS (
    SELECT * FROM 08_advanced_filtering
),
category_performance_data AS (
    SELECT * FROM 06_category_performance
),
value_aggregation_data AS (
    SELECT * FROM 03_value_aggregation
),
status_trend_data AS (
    SELECT * FROM 07_status_trend_analysis
)
SELECT 
    cp.category,
    cp.high_value_count,
    cp.total_high_value,
    va.item_count AS total_aggregate_count,
    va.total_value AS aggregate_total_value,
    st.total_items AS status_total_items,
    st.total_value AS status_total_value,
    COUNT(DISTINCT af.name) AS unique_advanced_filter_items,
    AVG(af.value) AS avg_advanced_filter_value
FROM 
    category_performance_data cp
LEFT JOIN 
    value_aggregation_data va ON cp.category = va.category
LEFT JOIN 
    status_trend_data st ON cp.category = st.status
LEFT JOIN 
    advanced_filter_data af ON cp.category = af.category
GROUP BY 
    cp.category, 
    cp.high_value_count, 
    cp.total_high_value,
    va.item_count,
    va.total_value,
    st.total_items,
    st.total_value
ORDER BY 
    cp.total_high_value DESC
LIMIT 10;