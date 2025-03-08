WITH cte1 AS (
            SELECT * FROM x.table1
        ),
        cte2 AS (
            SELECT * FROM schema2.table2
        )
        SELECT 
            cte1.*, 
            cte2.*,
            (SELECT COUNT(*) FROM schema3.lookup_table WHERE id = cte1.id)
        FROM cte1
        LEFT JOIN cte2 ON cte1.id = cte2.id