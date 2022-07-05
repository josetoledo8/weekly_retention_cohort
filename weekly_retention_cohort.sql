DECLARE min_week DATE;
DECLARE weeks STRING;
SET min_week = (
    SELECT MIN(DATE_TRUNC(DATE(created_at), WEEK))
    FROM BIGQUERY_PROJECT_NAME.DATABASE_NAME.USERS_TABLE
);
SET weeks = (
    SELECT CONCAT('("', STRING_AGG(CAST(x AS STRING), '", "'), '")')
    FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), min_week, WEEK))) AS x
);

EXECUTE IMMEDIATE FORMAT("""

WITH cohort_items AS (
    SELECT
        user_id,
        DATE_TRUNC(DATE(user_created_at), WEEK) AS cohort_week
    FROM BIGQUERY_PROJECT_NAME.DATABASE_NAME.USERS_TABLE
),
user_activities AS (
    SELECT
        A.user_id,
        DATE_DIFF(A.week, C.cohort_week, WEEK) AS week_number
    FROM (
        SELECT
            user_id,
            DATE_TRUNC(DATE(event_time), WEEK) AS week
        FROM BIGQUERY_PROJECT_NAME.DATABASE_ACTIVITIES.ACTIVIES_TABLE AS A
    ) AS A

    LEFT JOIN cohort_items AS C
        ON C.user_id = A.user_id
        
    WHERE A.user_id IS NOT NULL
),
cohort_size AS (
    SELECT
        cohort_week,
        COUNT(DISTINCT user_id) AS num_users
    FROM cohort_items
    GROUP BY 1
),
retention_table AS (
    SELECT
        C.cohort_week,
        A.week_number,
        COUNT(DISTINCT A.user_id) AS num_users
    FROM user_activities AS A
    LEFT JOIN cohort_items AS C 
        ON A.user_id = C.user_id
    group by 1, 2
),
cohorts AS (
    SELECT
        B.cohort_week,
        S.num_users AS total_users,
        CAST(B.week_number AS STRING) AS week_number,
        ROUND(SAFE_DIVIDE(B.num_users* 100, S.num_users), 3) AS percentage
    
    FROM retention_table AS B
    
    LEFT JOIN cohort_size AS S 
        ON B.cohort_week = S.cohort_week
    
    WHERE B.cohort_week IS NOT NULL AND B.week_number >= 0
    
    ORDER BY 1, 3
)
SELECT *
FROM cohorts PIVOT (
    SUM(percentage) AS _
    FOR week_number IN %s
)
ORDER BY cohort_week DESC

""", weeks);