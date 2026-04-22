--Phase 7:Integration Back to BI
--creating and loading "demand_forecast_future" , "SupplierRisk", 
--"demand_forecast_eval" and "stockout_risk"

USE SupplyChainProject;
GO

-- Creating 'phase7' schema to keep BI outputs separate
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'phase7')
BEGIN
    EXEC('CREATE SCHEMA phase7');
END

-- Creating Tables
-- Droping if already exists (clean reload)
DROP TABLE IF EXISTS phase7.demand_forecast_eval;
DROP TABLE IF EXISTS phase7.demand_forecast_future;
DROP TABLE IF EXISTS phase7.stockout_risk;
DROP TABLE IF EXISTS phase7.SupplierRisk;

GO

--demand_forecast_eval table
CREATE TABLE phase7.demand_forecast_eval (
    product_key INT,  date DATE, year INT, month INT,
    quarter INT, historical_demand FLOAT, predicted_demand FLOAT );
GO

--demand_forecast_future table
CREATE TABLE phase7.demand_forecast_future (
    product_key INT, date DATE, year INT, month INT, forecast_demand FLOAT);
GO

--stockout_risk table
CREATE TABLE phase7.stockout_risk (
    product_key INT, warehouse_key INT, supplier_name VARCHAR(100),
    current_stock FLOAT, reorder_level FLOAT, stock_ratio FLOAT,
    days_of_cover FLOAT, avg_lead_time FLOAT, avg_demand FLOAT,
    stockout_probability FLOAT, risk_label VARCHAR(20), stockout_flag INT);
GO

--supplier_risk table!
CREATE TABLE phase7.SupplierRisk
(
    product_key INT, supplier_key INT, quantity FLOAT, actual_delay_flag BIT,
    predicted_delay_flag BIT, delay_probability FLOAT, risk_label NVARCHAR(50),
    predicted_status NVARCHAR(50), actual_status NVARCHAR(50), supplier_id NVARCHAR(20),
    supplier_name NVARCHAR(100), rating FLOAT
);

-- Checking schema
SELECT * FROM sys.schemas WHERE name = 'phase7';

-- Checking tables
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'phase7';

--BULK INSERTING — ALL TABLES

--Demand Forecast (Evaluation)
BULK INSERT phase7.demand_forecast_eval
FROM 'C:\SQLData\phase7_demand_forecast_eval.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

--Demand Forecast (Future)
BULK INSERT phase7.demand_forecast_future
FROM 'C:\SQLData\phase7_demand_forecast_future.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

---Stock-Out Risk
BULK INSERT phase7.stockout_risk
FROM 'C:\SQLData\phase7_stockout_risk.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

--SupplierRisk
BULK INSERT phase7.SupplierRisk
FROM 'C:\SQLData\phase7_supplier_delay.csv' 
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

--verifying!
SELECT COUNT(*) FROM phase7.demand_forecast_eval;
SELECT COUNT(*) FROM phase7.demand_forecast_future;
SELECT COUNT(*) FROM phase7.stockout_risk;
SELECT COUNT(*) FROM phase7.SupplierRisk;

SELECT * FROM phase7.demand_forecast_eval;
SELECT * FROM phase7.demand_forecast_future;
SELECT * FROM phase7.stockout_risk;
SELECT * FROM phase7.SupplierRisk;