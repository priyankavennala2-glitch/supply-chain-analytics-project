
USE SupplyChainProject;
--PHASE 3 – SQL ANALYTICS

--1.Identification of suppliers contributing to maximum delivery delays.--
WITH supplier_delay AS (
    SELECT
        ds.supplier_name,
        DATEDIFF(DAY, d_promised.full_date, d_actual.full_date) AS delay_days
    FROM fact.Fact_Purchase fp
    JOIN dim.Dim_Supplier ds
        ON fp.supplier_key = ds.supplier_key
    JOIN dim.Dim_Date d_promised
        ON fp.promised_date_key = d_promised.date_key
    JOIN dim.Dim_Date d_actual
        ON fp.actual_date_key = d_actual.date_key
)

SELECT
    supplier_name,
    SUM(delay_days) AS total_delay_days
FROM supplier_delay
GROUP BY supplier_name
ORDER BY total_delay_days DESC;

----------------------
-- 2.Calculate average delivery delay per supplier
WITH supplier_delay AS (
    SELECT
        ds.supplier_name,
        DATEDIFF(DAY, d_promised.full_date, d_actual.full_date) AS delay_days
    FROM fact.Fact_Purchase fp
    JOIN dim.Dim_Supplier ds
        ON fp.supplier_key = ds.supplier_key
    JOIN dim.Dim_Date d_promised
        ON fp.promised_date_key = d_promised.date_key
    JOIN dim.Dim_Date d_actual
        ON fp.actual_date_key = d_actual.date_key
)

SELECT
    supplier_name,
    AVG(delay_days * 1.0) AS avg_delay_days
FROM supplier_delay
GROUP BY supplier_name
ORDER BY avg_delay_days DESC;
---------------------------------------

-- 3.Month-over-month logistics cost and efficiency.
WITH monthly_cost AS (
    SELECT
        dd.year,
        dd.month,
        SUM(fl.shipment_cost) AS total_logistics_cost
    FROM fact.Fact_Logistics fl
    JOIN fact.Fact_Purchase fp
        ON fl.purchase_fact_key = fp.purchase_fact_key
    JOIN dim.Dim_Date dd
        ON fp.order_date_key = dd.date_key
    GROUP BY dd.year, dd.month
)

SELECT
    year,
    month,
    total_logistics_cost,
    LAG(total_logistics_cost) OVER (ORDER BY year, month) AS prev_month_cost,
    total_logistics_cost -
    LAG(total_logistics_cost) OVER (ORDER BY year, month) AS mom_change
FROM monthly_cost
ORDER BY year, month;
------------------------------------------------

-- 4.Supplier performance ranking based on delivery.
WITH supplier_avg_delay AS (
    SELECT
        ds.supplier_name,
        AVG(DATEDIFF(DAY, d_promised.full_date, d_actual.full_date) * 1.0) AS avg_delay
    FROM fact.Fact_Purchase fp
    JOIN dim.Dim_Supplier ds
        ON fp.supplier_key = ds.supplier_key
    JOIN dim.Dim_Date d_promised
        ON fp.promised_date_key = d_promised.date_key
    JOIN dim.Dim_Date d_actual
        ON fp.actual_date_key = d_actual.date_key
    GROUP BY ds.supplier_name
)

SELECT
    supplier_name,
    avg_delay,
    RANK() OVER (ORDER BY avg_delay DESC) AS delay_rank
FROM supplier_avg_delay;
---------------------------------------------------------------

-- 5.Identify top 3 delayed suppliers per month.
WITH monthly_supplier_delay AS (
    SELECT
        dd.year,
        dd.month,
        ds.supplier_name,
        AVG(DATEDIFF(DAY, d_promised.full_date, d_actual.full_date) * 1.0) AS avg_delay
    FROM fact.Fact_Purchase fp
    JOIN dim.Dim_Supplier ds
        ON fp.supplier_key = ds.supplier_key
    JOIN dim.Dim_Date d_promised
        ON fp.promised_date_key = d_promised.date_key
    JOIN dim.Dim_Date d_actual
        ON fp.actual_date_key = d_actual.date_key
    JOIN dim.Dim_Date dd
        ON fp.order_date_key = dd.date_key
    GROUP BY dd.year, dd.month, ds.supplier_name
)

SELECT *
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY year, month ORDER BY avg_delay DESC) AS monthly_rank
    FROM monthly_supplier_delay
) ranked
WHERE monthly_rank <= 3
ORDER BY year, month, monthly_rank;
-----------------------------------------------------------

-- 6.Calculate average shipment cost per transport mode.
SELECT
    dt.transport_mode,
    AVG(fl.shipment_cost) AS avg_shipment_cost
FROM fact.Fact_Logistics fl
JOIN dim.Dim_Transport_Mode dt
    ON fl.transport_mode_key = dt.transport_mode_key
GROUP BY dt.transport_mode
ORDER BY avg_shipment_cost DESC;
