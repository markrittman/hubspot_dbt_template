{{
    config(
        materialized='table'
    )
}}
WITH event_type_seq_final AS (
    SELECT
        customer_id,
        user_session_id AS event_type_seq,
        event_type,
        event_ts,
        is_new_session
    FROM
        (SELECT
            customer_id,
            event_type,
            last_event,
            event_ts,
            is_new_session,
            SUM(is_new_session) OVER (ORDER BY customer_id ASC, event_ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS global_session_id,
            SUM(is_new_session) {{ customer_window_over('customer_id', 'event_ts', 'ASC') }} AS user_session_id
        FROM
            (SELECT
                *,
                CASE WHEN event_type != LAG(event_type, 1) OVER (PARTITION BY customer_id ORDER BY event_ts ASC) OR last_event IS NULL THEN 1
                ELSE 0
                END AS is_new_session
            FROM
                (SELECT
                    customer_id,
                    customer_name,
                    CASE WHEN event_type LIKE '%Email%' THEN 'Presales'
                    WHEN event_type LIKE '%Bill%' THEN 'Delivery' ELSE event_type END AS event_type,
                    event_ts,
                    LAG(CASE WHEN event_type LIKE '%Email%' THEN 'Presales' WHEN event_type LIKE '%Bill%' THEN 'Delivery' ELSE event_type END, 1) OVER (PARTITION BY customer_id ORDER BY event_ts ASC) AS last_event
                FROM
                    {{ ref('customer_events') }}
                ORDER BY
                    customer_id ASC,
                    event_ts ASC,
                    event_type ASC
                ) last
             ORDER BY
                customer_id ASC,
                event_ts ASC,
                event_type ASC
           ) final
        ORDER BY
            customer_id ASC,
            is_new_session DESC,
            event_ts ASC
        )
    WHERE
        is_new_session = 1
    {{ dbt_utils.group_by(n=5) }}
    ORDER BY
        customer_id,
        user_session_id,
        event_ts
)
SELECT
    customer_id,
    MAX(event_type_1) AS event_type_1,
    MAX(event_type_2) AS event_type_2,
    MAX(event_type_3) AS event_type_3,
    MAX(event_type_4) AS event_type_4,
    MAX(event_type_5) AS event_type_5,
    MAX(event_type_6) AS event_type_6,
    MAX(event_type_7) AS event_type_7,
    MAX(event_type_8) AS event_type_8,
    MAX(event_type_9) AS event_type_9,
    MAX(event_type_10) AS event_type_10
FROM
    (SELECT
        customer_id,
        CASE WHEN event_type_seq = 1 THEN event_type END AS event_type_1,
        CASE WHEN event_type_seq = 2 THEN event_type END AS event_type_2,
        CASE WHEN event_type_seq = 3 THEN event_type END AS event_type_3,
        CASE WHEN event_type_seq = 4 THEN event_type END AS event_type_4,
        CASE WHEN event_type_seq = 5 THEN event_type END AS event_type_5,
        CASE WHEN event_type_seq = 6 THEN event_type END AS event_type_6,
        CASE WHEN event_type_seq = 7 THEN event_type END AS event_type_7,
        CASE WHEN event_type_seq = 8 THEN event_type END AS event_type_8,
        CASE WHEN event_type_seq = 9 THEN event_type END AS event_type_9,
        CASE WHEN event_type_seq = 10 THEN event_type END AS event_type_10
    FROM
        event_type_seq_final
    )
{{ dbt_utils.group_by(n=1) }}
