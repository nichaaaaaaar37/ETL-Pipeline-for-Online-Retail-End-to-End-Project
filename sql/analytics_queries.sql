-- Top Products
SELECT Description, StockCode, SUM(Quantity) AS total_sold
FROM hip-catalyst-471911-a1.retail_dataset.online_retail_processed
GROUP BY 1,2
ORDER BY total_sold DESC
LIMIT 10;


-- Monthly Sales Trend
SELECT 
  FORMAT_DATE(
    '%Y-%m', 
    DATE(TIMESTAMP_MICROS(CAST(InvoiceDate/1000 AS INT64)))
  ) AS month,
  SUM(`Total Price`) AS monthly_sales
FROM hip-catalyst-471911-a1.retail_dataset.online_retail_processed
GROUP BY month
ORDER BY month;



