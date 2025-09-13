# ETL-Pipeline-for-Online-Retail-End-to-End-Project
End-to-End ETL Pipeline using Apache Airflow &amp; GCP : Cloud Composer, Google Cloud Storage, and BigQuery

# Retail ETL & Data Validation Pipeline

## 📌 Overview
This project is an **end-to-end data engineering pipeline** built on **Google Cloud Platform (GCP)**.  
It processes raw retail sales data, ensures data quality, and loads clean datasets into **BigQuery** for analytics and business insights.

---

## 🎯 Objectives
- Automate the ETL pipeline (Extract → Transform → Load).
- Apply **data validation checks** (duplicates, nulls, type consistency).
- Support business intelligence use cases such as:
  - Top customers by sales
  - Best-selling products
  - Monthly sales trends

---

## 🛠 Tech Stack
- **Python** (pandas, pyarrow)
- **Airflow / Cloud Composer** (orchestration & scheduling)
- **Google Cloud Storage (GCS)** (data lake / staging area)
- **BigQuery** (data warehouse & analytics)
- **SQL** (validation & insights)

---

## ⚙️ Pipeline Workflow

| Step | Task | Description |
|------|------|-------------|
| 1 | Load Data | Download CSV from GCS and save as Parquet |
| 2 | Drop Duplicates | Remove duplicate rows by `Invoice + StockCode + InvoiceDate` |
| 3 | Handle Missing Values | Fill nulls with defaults (`Unknown`, `0`) |
| 4 | Feature Engineering | Create `Total Price`, date features, weekend flag |
| 5 | Handle Outliers | Filter negative values, flag transactions > 1000 |
| 6 | Convert Data Types | Ensure correct schema (int, float, datetime) |
| 7 | Trim & Normalize Strings | Clean text fields (`Description`, `Country`, `StockCode`) |
| 8 | Upload to GCS | Save processed data back to GCS as Parquet |
| 9 | Load to BigQuery | Load final dataset into BigQuery for analytics |

---

## 🔎 Mapping Between Code & Workflow

| Code Function | Workflow Step | Description |
|---------------|---------------|-------------|
| `load_data` | Step 1 | Extract raw CSV from GCS, save as Parquet |
| `drop_duplicates` | Step 2 | Remove duplicate rows |
| `handle_missing_values` | Step 3 | Fill missing values with defaults |
| `feature_engineering` | Step 4 | Add features (`Total Price`, day of week, weekend) |
| `handle_outliers` | Step 5 | Filter invalid values, flag high-price rows |
| `convert_data_types` | Step 6 | Cast columns to correct types |
| `trim_and_normalize_strings` | Step 7 | Normalize string columns |
| `upload_to_gcs` | Step 8 | Upload cleaned Parquet to GCS |
| `load_to_bigquery` | Step 9 | Load Parquet into BigQuery table |

---

## 📊 Sample Validation Queries

**Top Customers by Sales**
```sql
SELECT
  Customer_ID,
  ROUND(SUM(Total_Price), 2) AS total_sales,
  SUM(Quantity) AS total_quantity
FROM `project_id.retail_dataset.online_retail_processed`
WHERE Customer_ID IS NOT NULL
GROUP BY Customer_ID
ORDER BY total_sales DESC
LIMIT 10;


