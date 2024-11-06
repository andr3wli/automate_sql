WITH recent_flights AS (
    SELECT
        flight_month,
        airline_code,
        flight_number,
        last_update_date
    FROM (
        SELECT
            RANK() OVER (PARTITION BY flight_month, airline_code, flight_number ORDER BY last_update_date DESC) AS rank,
            a.*
        FROM (
            SELECT DISTINCT
                airline_code,
                flight_number,
                flight_month,
                last_update_date
            FROM airline_data.flight_performance
        ) a
    )
    WHERE rank = 1
),

final_summary AS (
    SELECT
        a.airline_code,
        a.flight_number,
        a.flight_month,
        CASE
            WHEN EXTRACT(MONTH FROM a.flight_month) BETWEEN 1 AND 3 THEN (EXTRACT(YEAR FROM a.flight_month)-1)||'/'||EXTRACT(YEAR FROM a.flight_month)||' Q4'
            WHEN EXTRACT(MONTH FROM a.flight_month) BETWEEN 4 AND 6 THEN EXTRACT(YEAR FROM a.flight_month)||'/'||(EXTRACT(YEAR FROM a.flight_month)+1)||' Q1'
            WHEN EXTRACT(MONTH FROM a.flight_month) BETWEEN 7 AND 9 THEN EXTRACT(YEAR FROM a.flight_month)||'/'||(EXTRACT(YEAR FROM a.flight_month)+1)||' Q2'
            ELSE EXTRACT(YEAR FROM a.flight_month)||'/'||(EXTRACT(YEAR FROM a.flight_month)+1)||' Q3'
        END AS fiscal_quarter,
        COUNT(DISTINCT passenger_id) AS total_passengers,
        ROUND(AVG(flight_duration), 2) AS avg_flight_duration,
        ROUND(AVG(route_complexity), 2) AS avg_route_complexity,
        ROUND(SUM(total_revenue), 0) AS total_revenue,
        ROUND(SUM(international_flights), 0) AS international_flights,
        ROUND(SUM(domestic_flights), 0) AS domestic_flights,
        ROUND(SUM(total_flights), 0) AS total_flights,
        ROUND(SUM(net_revenue), 0) AS net_revenue,
        ROUND(SUM(bonus_payments), 0) AS bonus_payments
    FROM airline_data.flight_performance a
    INNER JOIN recent_flights b ON
        a.flight_month = b.flight_month
        AND a.airline_code = b.airline_code
        AND a.flight_number = b.flight_number
        AND a.last_update_date = b.last_update_date
    WHERE a.flight_month >= DATE '2020-10-01'
    GROUP BY
        a.airline_code,
        CASE
            WHEN EXTRACT(MONTH FROM a.flight_month) BETWEEN 1 AND 3 THEN (EXTRACT(YEAR FROM a.flight_month)-1)||'/'||EXTRACT(YEAR FROM a.flight_month)||' Q4'
            WHEN EXTRACT(MONTH FROM a.flight_month) BETWEEN 4 AND 6 THEN EXTRACT(YEAR FROM a.flight_month)||'/'||(EXTRACT(YEAR FROM a.flight_month)+1)||' Q1'
            WHEN EXTRACT(MONTH FROM a.flight_month) BETWEEN 7 AND 9 THEN EXTRACT(YEAR FROM a.flight_month)||'/'||(EXTRACT(YEAR FROM a.flight_month)+1)||' Q2'
            ELSE EXTRACT(YEAR FROM a.flight_month)||'/'||(EXTRACT(YEAR FROM a.flight_month)+1)||' Q3'
        END,
        ROLLUP(a.flight_month, a.flight_number)
    ORDER BY fiscal_quarter, a.airline_code, a.flight_number, a.flight_month
)

SELECT * FROM final_summary
WHERE (flight_number <> '00000' AND total_passengers >= 100) OR flight_number IS NULL
ORDER BY fiscal_quarter, flight_month, airline_code, flight_number;
