------Phase2_ETL----------

USE SupplyChainProject;

TRUNCATE TABLE fact.Fact_Logistics;
TRUNCATE TABLE fact.Fact_Production;
TRUNCATE TABLE fact.Fact_Inventory;
TRUNCATE TABLE fact.Fact_Purchase;
TRUNCATE TABLE fact.Fact_Sales_Forecast;

--cant use truncate for dim tables as it have fk for fact tables, so using delete instead!!.
DELETE FROM dim.Dim_Date;
DELETE FROM dim.Dim_Product;
DELETE FROM dim.Dim_Supplier;
DELETE FROM dim.Dim_Warehouse;
DELETE FROM dim.Dim_Transport_Mode;

--Loading Dim_Product--
-->Creates a clean master list of products (SKUs) from all operational sources.---
INSERT INTO dim.Dim_Product (sku)
SELECT DISTINCT sku FROM stg.inventory
WHERE sku IS NOT NULL

UNION
SELECT DISTINCT sku FROM stg.production
WHERE sku IS NOT NULL

UNION
SELECT DISTINCT sku FROM stg.purchase_orders
WHERE sku IS NOT NULL

UNION
SELECT DISTINCT sku FROM stg.sales_forecast
WHERE sku IS NOT NULL;

--Validating-- : )
SELECT COUNT(*) AS product_count FROM dim.Dim_Product;
SELECT TOP 10 * FROM dim.Dim_Product;

---------------
--Load Dim_Supplier
 --->Populates supplier master data.------
INSERT INTO dim.Dim_Supplier (supplier_id, supplier_name, location, rating)
SELECT Distinct supplier_id, supplier_name, location, rating
FROM stg.suppliers
WHERE supplier_id IS NOT NULL;

--Validating--
SELECT COUNT(*) AS supplier_count FROM dim.Dim_Supplier;
SELECT TOP 10 * FROM dim.Dim_Supplier;

----------------------------
--Loading Dim_Date--
-->Convert raw calendar strings into date keys.---
INSERT INTO dim.Dim_Date ( full_date, year, quarter, month, week,day, day_of_week)
SELECT CONVERT(DATE, full_date, 105), year, quarter, month, week, day, day_of_week
FROM stg.calendar
WHERE full_date IS NOT NULL;

--validating--
SELECT COUNT(*) AS date_count FROM dim.Dim_Date;
SELECT TOP 10 * FROM dim.Dim_Date ORDER BY full_date;

----------------
--Loading Dim_Warehouse--
INSERT INTO dim.Dim_Warehouse (warehouse_id)
SELECT DISTINCT warehouse_id
FROM stg.inventory
WHERE warehouse_id IS NOT NULL;

--validating--
SELECT * FROM dim.Dim_Warehouse;
----------------
SELECT warehouse_id, COUNT(*)
FROM dim.Dim_Warehouse
GROUP BY warehouse_id;
--Loading Dim_Transport_Mode
INSERT INTO dim.Dim_Transport_Mode (transport_mode)
SELECT DISTINCT transport_mode
FROM stg.logistics
WHERE transport_mode IS NOT NULL;

SELECT * FROM dim.Dim_Transport_Mode;
-----------------------------------------------
------------------------------------------------
SELECT 
  (SELECT COUNT(*) FROM dim.Dim_Product)       AS products,
  (SELECT COUNT(*) FROM dim.Dim_Supplier)      AS suppliers,
  (SELECT COUNT(*) FROM dim.Dim_Date)          AS dates,
  (SELECT COUNT(*) FROM dim.Dim_Warehouse)     AS warehouses,
  (SELECT COUNT(*) FROM dim.Dim_Transport_Mode) AS transport_modes;
--===================================================================

-----------Fact_Inventory ETL---------------------
--Data validation--
-- Checking for nulls or bad records-- expected :0 rows--
SELECT *
FROM stg.inventory
WHERE sku IS NULL   OR warehouse_id IS NULL   OR supplier_id IS NULL;

--Deduplication check--
SELECT sku, warehouse_id, supplier_id, COUNT(*) AS cnt
FROM stg.inventory
GROUP BY sku, warehouse_id, supplier_id
HAVING COUNT(*) > 1;

--Loading Fact_Inventory--
INSERT INTO fact.Fact_Inventory (product_key, warehouse_key, supplier_key, current_stock, reorder_level)
SELECT DISTINCT dp.product_key, dw.warehouse_key, ds.supplier_key,
CAST(si.current_stock AS INT), CAST(si.reorder_level AS INT)
FROM stg.inventory si
JOIN dim.Dim_Product dp ON si.sku = dp.sku
JOIN dim.Dim_Warehouse dw ON si.warehouse_id = dw.warehouse_id
JOIN dim.Dim_Supplier ds ON si.supplier_id = ds.supplier_id
WHERE si.current_stock IS NOT NULL AND si.reorder_level IS NOT NULL;

---validating--
-- Row count
SELECT COUNT(*) AS inventory_rows
FROM fact.Fact_Inventory;

-- Sample records
SELECT TOP 10 *
FROM fact.Fact_Inventory;

-- FK integrity check
SELECT *
FROM fact.Fact_Inventory fi
LEFT JOIN dim.Dim_Product dp ON fi.product_key = dp.product_key
WHERE dp.product_key IS NULL;


--------------------Fact_Production ETL------------------------
--validating---
--Validate staging data
SELECT *
FROM stg.production
WHERE sku IS NULL
   OR production_date IS NULL;

--validating date--
SELECT DISTINCT production_date
FROM stg.production
WHERE TRY_CONVERT(DATE, production_date) IS NULL;
 
--Loading Fact_Production (DD-MM-YYYY safe)
INSERT INTO fact.Fact_Production
(
    product_key,
    date_key,
    output_quantity,
    downtime_hours,
    downtime_reason
)
SELECT
    dp.product_key,
    dd.date_key,
    CAST(sp.output_quantity AS INT),
    CAST(sp.downtime_hours AS DECIMAL(5,2)),
    sp.downtime_reason
FROM stg.production sp
JOIN dim.Dim_Product dp
    ON sp.sku = dp.sku
JOIN dim.Dim_Date dd
    ON TRY_CONVERT(DATE, sp.production_date, 105) = dd.full_date
WHERE sp.output_quantity IS NOT NULL
  AND TRY_CONVERT(DATE, sp.production_date, 105) IS NOT NULL;

  SELECT COUNT(*) AS production_rows
FROM fact.Fact_Production;

SELECT TOP 10 *
FROM fact.Fact_Production;

-------------------Fact_Purchase ETL-----------------------------------
--Check bad dates 
SELECT DISTINCT order_date
FROM stg.purchase_orders
WHERE TRY_CONVERT(DATE, order_date, 105) IS NULL;

--Loading Fact_Purchase 
INSERT INTO fact.Fact_Purchase
(
    product_key,
    supplier_key,
    order_date_key,
    promised_date_key,
    actual_date_key,
    quantity
)
SELECT
    dp.product_key,
    ds.supplier_key,
    d_order.date_key,
    d_promised.date_key,
    d_actual.date_key,
    CAST(sp.quantity AS INT)
FROM stg.purchase_orders sp
JOIN dim.Dim_Product dp
    ON sp.sku = dp.sku
JOIN dim.Dim_Supplier ds
    ON sp.supplier_id = ds.supplier_id
JOIN dim.Dim_Date d_order
    ON TRY_CONVERT(DATE, sp.order_date, 105) = d_order.full_date
JOIN dim.Dim_Date d_promised
    ON TRY_CONVERT(DATE, sp.promised_delivery_date, 105) = d_promised.full_date
JOIN dim.Dim_Date d_actual
    ON TRY_CONVERT(DATE, sp.actual_delivery_date, 105) = d_actual.full_date
WHERE TRY_CONVERT(DATE, sp.order_date, 105) IS NOT NULL
  AND TRY_CONVERT(DATE, sp.promised_delivery_date, 105) IS NOT NULL
  AND TRY_CONVERT(DATE, sp.actual_delivery_date, 105) IS NOT NULL;

--validating--
SELECT COUNT(*) AS purchase_rows
FROM fact.Fact_Purchase;

SELECT TOP 10 *
FROM fact.Fact_Purchase;

-----------Fact_Sales_Forecast ETL------------------------
--Loading Fact_Sales_Forecast
INSERT INTO fact.Fact_Sales_Forecast
(
    product_key,
    date_key,
    historical_demand,
    forecasted_demand
)
SELECT
    dp.product_key,
    dd.date_key,
    sf.historical_demand,
    sf.forecasted_demand
FROM stg.sales_forecast sf
JOIN dim.Dim_Product dp
    ON sf.sku = dp.sku
JOIN dim.Dim_Date dd
    ON TRY_CONVERT(DATE, sf.forecast_date, 105) = dd.full_date
WHERE TRY_CONVERT(DATE, sf.forecast_date, 105) IS NOT NULL;

--validating---
SELECT COUNT(*) AS forecast_rows
FROM fact.Fact_Sales_Forecast;

SELECT TOP 10 *
FROM fact.Fact_Sales_Forecast;

------------------Fact_Logistics ETL-----------------------
--Loading Fact_Logistics
INSERT INTO fact.Fact_Logistics
(
    purchase_fact_key,
    transport_mode_key,
    shipment_cost,
    transit_delay_days
)
SELECT
    fp.purchase_fact_key,
    dt.transport_mode_key,
    CAST(sl.shipment_cost AS DECIMAL(10,2)),
    CAST(sl.transit_delay_days AS INT)
FROM stg.logistics sl
JOIN stg.purchase_orders spo
    ON sl.purchase_order_id = spo.purchase_order_id
JOIN fact.Fact_Purchase fp
    ON TRY_CONVERT(DATE, spo.order_date, 105) =
       (SELECT full_date FROM dim.Dim_Date WHERE date_key = fp.order_date_key)
JOIN dim.Dim_Transport_Mode dt
    ON sl.transport_mode = dt.transport_mode
WHERE sl.shipment_cost IS NOT NULL;

--validating--
SELECT COUNT(*) AS logistics_rows
FROM fact.Fact_Logistics;

SELECT TOP 10 *
FROM fact.Fact_Logistics;


/*===============================
   PHASE 2 – DERIVED METRICS
   Row-Level Transformations
   =============================== */

  /*inventory turnover, order fill rate, supplier on-time delivery percentage,
	logistics cost percentage, production downtime ratio, and lead  time variability.*/

	-- Creating analytics schema--
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC('CREATE SCHEMA analytics');
END
GO

---1.inventory turnover-------------
CREATE OR ALTER VIEW analytics.v_inventory_turnover AS
SELECT
    SUM(sf.historical_demand) * 1.0 /
    NULLIF(AVG(fi.current_stock),0) AS inventory_turnover
FROM fact.Fact_Sales_Forecast sf
JOIN fact.Fact_Inventory fi
    ON sf.product_key = fi.product_key;

----2.Order Fill Rate---
CREATE OR ALTER VIEW analytics.v_order_fill_rate AS
SELECT
    SUM(CASE WHEN fi.current_stock >= fp.quantity THEN 1 ELSE 0 END) * 100.0
    / COUNT(*) AS order_fill_rate_pct
FROM fact.Fact_Purchase fp
JOIN fact.Fact_Inventory fi
    ON fp.product_key = fi.product_key;

----3.Supplier On-Time Delivery %----- Delivery delay = Actual Date - Promised Date
CREATE OR ALTER VIEW analytics.v_supplier_on_time_pct AS
SELECT
    SUM(CASE 
        WHEN DATEDIFF(DAY, d_promised.full_date, d_actual.full_date) <= 0
        THEN 1 ELSE 0 END) * 100.0 / COUNT(*) 
        AS on_time_delivery_pct
FROM fact.Fact_Purchase fp
JOIN dim.Dim_Date d_promised 
    ON fp.promised_date_key = d_promised.date_key
JOIN dim.Dim_Date d_actual 
    ON fp.actual_date_key = d_actual.date_key;

----4.Logistics Cost Percentage----
CREATE OR ALTER VIEW analytics.v_logistics_cost_pct AS
SELECT
    SUM(fl.shipment_cost) * 100.0 /
    NULLIF(SUM(fp.quantity),0) AS logistics_cost_pct
FROM fact.Fact_Logistics fl
JOIN fact.Fact_Purchase fp
    ON fl.purchase_fact_key = fp.purchase_fact_key;

----5️.Production Downtime Ratio-----
CREATE OR ALTER VIEW analytics.v_production_downtime_ratio AS
SELECT
    SUM(downtime_hours) * 1.0 /
    NULLIF(SUM(output_quantity),0) AS downtime_ratio
FROM fact.Fact_Production;

------ 6️.Lead Time Variability------- Lead Time = Actual Date - Order Date
CREATE OR ALTER VIEW analytics.v_lead_time_variability AS
SELECT
    AVG(DATEDIFF(DAY, d_order.full_date, d_actual.full_date)) AS avg_lead_time,
    STDEV(DATEDIFF(DAY, d_order.full_date, d_actual.full_date)) AS lead_time_variability
FROM fact.Fact_Purchase fp
JOIN dim.Dim_Date d_order
    ON fp.order_date_key = d_order.date_key
JOIN dim.Dim_Date d_actual
    ON fp.actual_date_key = d_actual.date_key;


---------------------
SELECT * FROM analytics.v_inventory_turnover;
SELECT * FROM analytics.v_order_fill_rate;
SELECT * FROM analytics.v_supplier_on_time_pct;
SELECT * FROM analytics.v_logistics_cost_pct;
SELECT * FROM analytics.v_production_downtime_ratio;
SELECT * FROM analytics.v_lead_time_variability;
