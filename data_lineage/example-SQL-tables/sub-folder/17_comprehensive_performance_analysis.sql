-- Comprehensive performance analysis across multiple models
WITH value_aggregation_data AS (
    SELECT * FROM 03_value_aggregation
),
filtered_categories_data AS (
    SELECT * FROM 02_processed_source_data
),
status_trend_data AS (
    SELECT * FROM 07_status_trend_analysis
)
SELECT 
    va.category,
    va.item_count AS total_aggregate_count,
    va.total_value AS aggregate_total_value,
    fc.item_count AS filtered_item_count,
    fc.avg_value AS filtered_avg_value,
    st.total_items AS status_total_items,
    st.total_value AS status_total_value,
    CASE 
        WHEN va.total_value > st.total_value THEN 'Overperforming'
        WHEN va.total_value < st.total_value THEN 'Underperforming'
        ELSE 'Neutral'
    END AS performance_status
FROM 
    value_aggregation_data va
FULL OUTER JOIN 
    filtered_categories_data fc ON va.category = fc.category
FULL OUTER JOIN 
    status_trend_data st ON va.category = st.status
ORDER BY 
    va.total_value DESC
LIMIT 15;