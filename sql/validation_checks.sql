-- Duplicate Check
SELECT InvoiceNo, StockCode, COUNT(*) AS cnt
FROM `project_id.retail_dataset.online_retail_processed`
GROUP BY InvoiceNo, StockCode
HAVING COUNT(*) > 1;

-- Null Check
SELECT COUNT(*) AS null_customers
FROM `project_id.retail_dataset.online_retail_processed`
WHERE Customer_ID IS NULL;

-- Negative Values
SELECT COUNT(*) AS invalid_records
FROM `project_id.retail_dataset.online_retail_processed`
WHERE Quantity < 0 OR UnitPrice < 0;
