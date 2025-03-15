-- Combined analysis using multiple previous models
WITH advanced_filter_data AS (
    SELECT * FROM file_schema.e08_advanced_filtering
),
category_performance_data AS (
    SELECT * FROM file_schema.e06_category_performance
),
status_trend_data AS (
    SELECT * FROM file_schema.e07_status_trend_analysis
)
SELECT 
    af.category,
    af.name,
    af.value AS item_value,
    af.status,
    cp.high_value_count,
    cp.total_high_value AS category_total_value,
    st.total_items AS status_total_items,
    st.total_value AS status_total_value
FROM 
    advanced_filter_data af
JOIN 
    category_performance_data cp ON af.category = cp.category
JOIN 
    status_trend_data st ON af.status = st.status
ORDER BY 
    af.value DESC
LIMIT 20;