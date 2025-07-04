# 🏆 KPI: Top 3 Most Demanded Categories

## 🎯 Objective
Identify the three product categories with the highest total quantity ordered.

---

## 💼 Business Value
This KPI is essential for:
- Strategic assortment planning.
- Guiding category-level marketing investments.
- Allocating inventory and shelf space based on performance.

---

## 🧮 Query

```sql
SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category
ORDER BY total_quantity DESC
LIMIT 3;
```

---

## 📌 Notes & Recommendations
- Monitor how this ranking changes over time (e.g., monthly snapshots).
- Combine with margin data (if available) to prioritize by profit, not just volume.
- Consider visualizing this as a horizontal bar chart or pie chart in dashboards.
- Use as input for ML models related to pricing, bundling, or inventory allocation.

---

# 🥇 KPI: Most Demanded Product

## 🎯 Objective
Identify the single product that has accumulated the highest total quantity ordered.

---

## 💼 Business Value
Understanding the top-performing product allows:
- Prioritization in promotions and restocking.
- Identifying customer preferences.
- Benchmarking other products against the top performer.

---

## 🧮 Query

```sql
SELECT p.name
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.name
ORDER BY SUM(o.quantity) DESC
LIMIT 1;
```

---

## 📌 Notes & Recommendations
- To display total quantity, include `SUM(o.quantity) AS total_quantity` in the SELECT.
- Ensure `product_id` and `quantity` are not null for accurate results.
- Useful for leaderboard-style dashboards or performance reports.
- Add time filters (e.g., last 30 days) to make this KPI dynamic.

---

# 🗓️ KPI: Peak Order Date

## 🎯 Objective
Identify the single date with the highest number of orders registered in the system.

---

## 💼 Business Value
This KPI helps:
- Detect exceptional sales dates (e.g., Black Friday, product launches).
- Inform future campaign planning using historical performance.
- Understand peak operational demand for staffing and logistics.

---

## 🧮 Query

```sql
SELECT created_date
FROM orders
GROUP BY created_date
ORDER BY COUNT(*) DESC
LIMIT 1;
```

---

## 📌 Notes & Recommendations
- Add a second column like `COUNT(*) AS total_orders` to report volume explicitly.
- Pair with metadata (e.g., promotions, holidays) to explain peaks.
- Create an index on `created_date` for faster grouping and sorting in large datasets.
- Use this KPI periodically (e.g., monthly) to discover new trends.

---

# 🚨 KPI: Stockout Risk Index

## 🎯 Objective
Estimate the risk of product stockouts by comparing current inventory levels to recent demand.

---

## 💼 Business Value
This KPI is critical for:
- Preventing lost sales due to out-of-stock items.
- Improving supplier coordination and replenishment cycles.
- Prioritizing restocking for high-demand, low-inventory products.

---

## 📊 Required Tables
- `orders(product_id, quantity, created_date)`
- `products(id, name)`
- `inventory(product_id, current_stock)`

---

## 🧮 Query

```sql
SELECT 
  p.name,
  i.current_stock,
  SUM(o.quantity) AS demand_last_30d,
  ROUND(
    i.current_stock::float / NULLIF(SUM(o.quantity), 0), 2
  ) AS stock_coverage_ratio
FROM orders o
JOIN products p ON o.product_id = p.id
JOIN inventory i ON p.id = i.product_id
WHERE o.created_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.name, i.current_stock
ORDER BY stock_coverage_ratio ASC;
```

---

## 📌 Notes & Recommendations
- A ratio `< 1` indicates likely stockout risk; `< 0.5` signals urgent action.
- Automate alerts for SKUs with low ratios.
- Optionally, weight demand by recency to better model real-world consumption.
- Ensure the `inventory` table is updated frequently to reflect real-time stock levels.

---

# 📆 KPI: Category Seasonality Analysis

## 🎯 Objective
Measure monthly demand patterns for each product category to detect seasonality and cyclical behavior.

---

## 💼 Business Value
This KPI supports strategic decisions such as:
- Campaign planning around high-demand months.
- Forecasting demand spikes and allocating inventory accordingly.
- Identifying slow months to push stock through promotions.

---

## 🧮 Query

```sql
SELECT 
  p.category, 
  EXTRACT(MONTH FROM o.created_date::date) AS month, 
  SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category, month
ORDER BY p.category, month;
```

---

## 📌 Notes & Recommendations
- Consider building a pivot table from this query to visualize category trends over 12 months.
- Replace `MONTH` with `WEEK` or `DAYOFWEEK` for shorter-term seasonal trends.
- For long-term planning, run this KPI across multiple years to validate repeatable patterns.
- Index `created_date` and `product_id` to improve performance in large datasets.

---

# 🔁 KPI: Purchase Frequency per Product

## 🎯 Objective
Identify how frequently each product is purchased based on the number of distinct purchase days.

---

## 💼 Business Value
This KPI helps distinguish between high-rotation and low-rotation products. It enables:
- Better inventory management by identifying stable vs. sporadic demand.
- Marketing segmentation for recurring-purchase products.
- Prioritization of high-frequency SKUs for availability and visibility.

---

## 🧮 Query

```sql
SELECT product_id, COUNT(DISTINCT created_date) AS purchase_days
FROM orders
GROUP BY product_id
ORDER BY purchase_days DESC;
```

---

## 📌 Notes & Recommendations
- Join with `products` to retrieve names and categories for readability.
- Index `created_date` and `product_id` for large-scale performance.
- Optionally, use a rolling time window (e.g., last 90 days) for dynamic frequency tracking.

---

## 🔄 ETL/ELT Tool for BigQuery Integration

**Recommended Tool:** Apache Airflow (or Cloud Composer on GCP)

**Why:**  
Airflow is ideal for building reliable, scalable, and automated data pipelines. It integrates seamlessly with GCP services like Cloud Storage, Dataproc and BigQuery.

**Disclaimer**
We can build all this following pipeline according to a medallion architecture (bronze, silver, golden) writing this staging tables or files in each schema.

### 🧱 Steps to Build the Pipeline
1. Cluster provision
   - Use `DataprocCreateClusterOperator` to create a cluster in Google Cloud Dataproc
   - Create an init.sh script to run at cluster creation.

2. **Extract**  
   - Use `DataprocSubmitJobOperator` to fetch raw CSVs from a local source or GCS bucket using Pyspark.

3. **Transform**  
   - Merge and enrich data using `Pyspark` in `DataprocSubmitJobOperator` tasks.
   - Convert currency values using external APIs (as shown in Challenge 2).

4. **Load**  
   - Use `DataprocSubmitJobOperator` to upload cleaned files to a GCS bucket, then load to BigQuery.
  
5. **Delete Cluster**
   - Use `DataprocDeleteClusterOperator` to delete the cluster at the end of the process. 

6. **Schedule**  
   - Set up DAGs to run daily/weekly.
   - Monitor with Airflow UI or alerts.

---

## 🤖 AI-Based Pipeline Proposal

**Objective:** Forecast product demand using machine learning.

### 🔍 Why this adds value:
- Improves inventory accuracy
- Helps avoid stockouts or overstock
- Optimizes pricing and promotions

### ⚙️ Pipeline Architecture

1. **Data Sources**
   - Historical `orders` + `products` data
   - Enriched with external variables (e.g. holidays, campaigns)

2. **Preprocessing**
   - Aggregate order volume per product by day/week
   - Normalize quantities and engineer time-based features

3. **Modeling**
   - Use time series models such as:
     - Facebook Prophet
     - XGBoost with lag features
     - LSTM (for large datasets)

4. **Serving**
   - Export predictions to BigQuery
   - Build Looker/Power BI dashboards with forecast overlays

5. **Automation**
   - Retrain models monthly using Airflow
   - Validate performance via MAPE or RMSE

This ML pipeline would convert raw transactional data into future-ready business intelligence.


