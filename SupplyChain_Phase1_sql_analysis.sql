--Database & Schemas--
CREATE DATABASE SupplyChainProject;
USE SupplyChainProject;

CREATE SCHEMA stg;
GO
CREATE SCHEMA dim;
GO
CREATE SCHEMA fact;
GO
---------------------------------------------------------
---Creating and loading Staging Tables---------
--staging tables-- (7)
--stg.inventory; stg.calendar; stg.suppliers ; stg.production; stg.purchase_orders
-- stg.sales_forecast ; stg.logistics

-- =========================
-- 1.STAGING: INVENTORY-----
DROP TABLE IF EXISTS stg.inventory;
CREATE TABLE stg.inventory (
    sku VARCHAR(50),
    warehouse_id VARCHAR(50),
    supplier_id VARCHAR(20),
    current_stock INT,
    reorder_level INT
);

BULK INSERT stg.inventory
FROM 'C:\SQLData\inventory.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');

-- 2.STAGING: CALENDAR------
DROP TABLE IF EXISTS stg.calendar;
CREATE TABLE stg.calendar (
    full_date VARCHAR(20),
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    day_of_week VARCHAR(20)
);

BULK INSERT stg.calendar
FROM 'C:\SQLData\calendar.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');

-- 3.STAGING: SUPPLIERS------
DROP TABLE IF EXISTS stg.suppliers;
CREATE TABLE stg.suppliers (
    supplier_id VARCHAR(20),
    supplier_name VARCHAR(100),
    location VARCHAR(50),
    rating FLOAT
);

BULK INSERT stg.suppliers
FROM 'C:\SQLData\suppliers.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');

-- 4.STAGING: PRODUCTION----
DROP TABLE IF EXISTS stg.production;
CREATE TABLE stg.production (
    production_id VARCHAR(20),
    production_date VARCHAR(20),
    sku VARCHAR(20),
    output_quantity INT,
    downtime_hours DECIMAL(5,2),
    downtime_reason VARCHAR(50)
);

BULK INSERT stg.production
FROM 'C:\SQLData\production.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');


-- 5.STAGING: PURCHASE ORDERS---
DROP TABLE IF EXISTS stg.purchase_orders;
CREATE TABLE stg.purchase_orders (
    purchase_order_id VARCHAR(20),
    order_date VARCHAR(20),
    sku VARCHAR(20),
    supplier_id VARCHAR(20),
    quantity INT,
    promised_delivery_date VARCHAR(20),
    actual_delivery_date VARCHAR(20)
);

BULK INSERT stg.purchase_orders
FROM 'C:\SQLData\purchase_orders.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');

-- 6.STAGING: SALES FORECAST---------------
DROP TABLE IF EXISTS stg.sales_forecast;
CREATE TABLE stg.sales_forecast (
    forecast_date VARCHAR(20),
    sku VARCHAR(20),
    historical_demand INT,
    forecasted_demand INT
);

BULK INSERT stg.sales_forecast
FROM 'C:\SQLData\sales_forecast.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');


-- 7.STAGING: LOGISTICS----
DROP TABLE IF EXISTS stg.logistics;
CREATE TABLE stg.logistics (
    shipment_id VARCHAR(20),
    purchase_order_id VARCHAR(20),
    transport_mode VARCHAR(20),
    shipment_cost DECIMAL(10,2),
    transit_delay_days INT
);

BULK INSERT stg.logistics
FROM 'C:\SQLData\logistics.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');

SELECT COUNT(*) FROM stg.inventory;
SELECT COUNT(*) FROM stg.calendar;
SELECT COUNT(*) FROM stg.suppliers;
SELECT COUNT(*) FROM stg.production;
SELECT COUNT(*) FROM stg.purchase_orders;
SELECT COUNT(*) FROM stg.sales_forecast;
SELECT COUNT(*) FROM stg.logistics;
--===========================================================================

--Creating Dimension Tables-- (5)
--Dim_Product; Dim_Supplier ; Dim_Date; Dim_Transport_Mode; Dim_Warehouse

--Dim_Product---
CREATE TABLE dim.Dim_Product (
    product_key INT IDENTITY PRIMARY KEY,
    sku VARCHAR(50)
);

--Dim_Supplier--
CREATE TABLE dim.Dim_Supplier (  
    supplier_key INT IDENTITY PRIMARY KEY,
    supplier_id VARCHAR(20),
    supplier_name VARCHAR(100),
    location VARCHAR(50),
    rating FLOAT
);

--Dim_Date--
CREATE TABLE dim.Dim_Date (  
    date_key INT IDENTITY PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    day_of_week VARCHAR(20)
);

--Dim_Transport_Mode--
CREATE TABLE dim.Dim_Transport_Mode (   
    transport_mode_key INT IDENTITY PRIMARY KEY,
    transport_mode VARCHAR(20)
);

--Dim_Warehouse--
CREATE TABLE dim.Dim_Warehouse ( 
    warehouse_key INT IDENTITY PRIMARY KEY,
    warehouse_id VARCHAR(20)
);

--===========================================================================
--Fact Tables (5)---
--Fact_Inventory; Fact_Production ; Fact_Purchase; Fact_Sales_Forecast; Fact_Logistics

--Fact_Inventory--
CREATE TABLE fact.Fact_Inventory (
    inventory_key INT IDENTITY PRIMARY KEY,
    product_key INT,
    warehouse_key INT,
    supplier_key INT,
    current_stock INT,
    reorder_level INT
);

--Fact_Production--
CREATE TABLE fact.Fact_Production ( 
    production_key INT IDENTITY PRIMARY KEY,
    product_key INT,
    date_key INT,
    output_quantity INT,
    downtime_hours DECIMAL(5,2),
    downtime_reason VARCHAR(100)
);

--Fact_Purchase--
CREATE TABLE fact.Fact_Purchase ( 
    purchase_fact_key INT IDENTITY PRIMARY KEY,
    product_key INT,
    supplier_key INT,
    order_date_key INT,
    promised_date_key INT,
    actual_date_key INT,
    quantity INT
);

--Fact_Sales_Forecast--
CREATE TABLE fact.Fact_Sales_Forecast ( 
    sales_forecast_key INT IDENTITY PRIMARY KEY,
    product_key INT,
    date_key INT,
    historical_demand INT,
    forecasted_demand INT
);

--Fact_Logistics--
CREATE TABLE fact.Fact_Logistics (
    logistics_fact_key INT IDENTITY PRIMARY KEY,
    purchase_fact_key INT,
    transport_mode_key INT,
    shipment_cost DECIMAL(10,2),
    transit_delay_days INT
);
---------------------------------------------------------------

SELECT COUNT(*) FROM stg.inventory;
SELECT COUNT(*) FROM dim.Dim_Product;   
SELECT COUNT(*) FROM fact.Fact_Purchase; 
-------------------------------------------------------------------
--for connections in ER ---
--Product relationships--
ALTER TABLE fact.Fact_Inventory
ADD CONSTRAINT FK_FI_Product
FOREIGN KEY (product_key) REFERENCES dim.Dim_Product(product_key);

ALTER TABLE fact.Fact_Production
ADD CONSTRAINT FK_FP_Product
FOREIGN KEY (product_key) REFERENCES dim.Dim_Product(product_key);

ALTER TABLE fact.Fact_Purchase
ADD CONSTRAINT FK_FPU_Product
FOREIGN KEY (product_key) REFERENCES dim.Dim_Product(product_key);

ALTER TABLE fact.Fact_Sales_Forecast
ADD CONSTRAINT FK_FSF_Product
FOREIGN KEY (product_key) REFERENCES dim.Dim_Product(product_key);

--Supplier relationships--
ALTER TABLE fact.Fact_Inventory
ADD CONSTRAINT FK_FI_Supplier
FOREIGN KEY (supplier_key) REFERENCES dim.Dim_Supplier(supplier_key);

ALTER TABLE fact.Fact_Purchase
ADD CONSTRAINT FK_FPU_Supplier
FOREIGN KEY (supplier_key) REFERENCES dim.Dim_Supplier(supplier_key);

--Date relationships--
ALTER TABLE fact.Fact_Production
ADD CONSTRAINT FK_FP_Date
FOREIGN KEY (date_key) REFERENCES dim.Dim_Date(date_key);

ALTER TABLE fact.Fact_Purchase
ADD CONSTRAINT FK_FPU_OrderDate
FOREIGN KEY (order_date_key) REFERENCES dim.Dim_Date(date_key);

ALTER TABLE fact.Fact_Purchase
ADD CONSTRAINT FK_FPU_PromisedDate
FOREIGN KEY (promised_date_key) REFERENCES dim.Dim_Date(date_key);

ALTER TABLE fact.Fact_Purchase
ADD CONSTRAINT FK_FPU_ActualDate
FOREIGN KEY (actual_date_key) REFERENCES dim.Dim_Date(date_key);

ALTER TABLE fact.Fact_Sales_Forecast
ADD CONSTRAINT FK_FSF_Date
FOREIGN KEY (date_key) REFERENCES dim.Dim_Date(date_key);

--Warehouse & Transport--
ALTER TABLE fact.Fact_Inventory
ADD CONSTRAINT FK_FI_Warehouse
FOREIGN KEY (warehouse_key) REFERENCES dim.Dim_Warehouse(warehouse_key);

ALTER TABLE fact.Fact_Logistics
ADD CONSTRAINT FK_FL_Transport
FOREIGN KEY (transport_mode_key) REFERENCES dim.Dim_Transport_Mode(transport_mode_key);
-----------------

select * from dim.Dim_Product;
select * from dim.Dim_Supplier;
select * from dim.Dim_Date;
select * from dim.Dim_Transport_Mode;
select * from dim.Dim_Warehouse;

select * from fact.Fact_Logistics;
select * from fact.Fact_Inventory;
select * from fact.Fact_Production;
select * from fact.Fact_Purchase;
select * from fact.Fact_Sales_Forecast;