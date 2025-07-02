# 🛠️ PostgreSQL Deployment & KPI Extraction on Google Cloud VM

## 🚀 Step 1: Create a Virtual Machine

Use Google Cloud Compute Engine to spin up a virtual machine:

```bash
gcloud compute instances create zubale
gcloud compute ssh zubale
```

---

## 🐳 Step 2: Install Docker and Run PostgreSQL

Switch to root and install Docker:

```bash
sudo su
apt update
apt install docker.io -y
```

Run a PostgreSQL container:

```bash
sudo docker run --name zubale-db \
-e POSTGRES_USER=ruben \
-e POSTGRES_PASSWORD=1234 \
-e POSTGRES_DB=zubale-db \
-d -p 5432:5432 postgres
```

Validate the running Docker image:

```bash
docker images
```

---

## 🔐 Step 3: Open Firewall Port for PostgreSQL

Allow TCP traffic on port 5432:

```bash
gcloud compute firewall-rules create allow-postgres --allow=tcp:5432
```

---

## 🗃️ Step 4: Enter the PostgreSQL Container

Connect to the database:

```bash
sudo docker exec -it zubale-db psql -U ruben -d zubale-db
```

Create a new database:

```sql
CREATE DATABASE zubale;
```

---

## ☁️ Step 5: Copy CSV Files from Google Cloud Storage

Download CSVs to local VM:

```bash
gsutil cp gs://zubale/zubale-orders.csv orders.csv
gsutil cp gs://zubale/zubale-products.csv products.csv
```

Copy CSVs into the container:

```bash
sudo docker cp products.csv zubale-db:/products.csv
sudo docker cp orders.csv zubale-db:/orders.csv
```

---

## 🛠️ Step 6: Create Tables Using DDL

Re-enter the container:

```bash
sudo docker exec -it zubale-db psql -U ruben -d zubale
```

Create the required tables:

```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    product_id INT,
    quantity INT,
    created_date TEXT
);
```

Verify tables:

```sql
\dt
```

---

## 📥 Step 7: Insert CSV Data into Tables

```sql
\COPY products FROM '/products.csv' DELIMITER ',' CSV HEADER;
\COPY orders FROM '/orders.csv' DELIMITER ',' CSV HEADER;
```

---

## 📊 Step 8: Run KPI Queries

### 1. 📅 Date with Maximum Orders

```sql
CREATE INDEX idx_orders_created_date ON orders(created_date);

SELECT created_date
FROM orders
GROUP BY created_date
ORDER BY COUNT(*) DESC
LIMIT 1;
```

### 2. 🥇 Most Demanded Product

```sql
CREATE INDEX idx_orders_product_id ON orders(product_id);
CREATE INDEX idx_products_id ON products(id);  

SELECT p.name
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.name
ORDER BY SUM(o.quantity) DESC
LIMIT 1;
```

### 3. 📦 Top 3 Most Demanded Categories

```sql
CREATE INDEX idx_products_category ON products(category);

SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category
ORDER BY total_quantity DESC
LIMIT 3;
```

---

## ✅ Notes
- Indexes are used to optimize query performance.
- Queries `select` only the value of the answer for each question to reduce resource consumption.
- ![image](https://github.com/user-attachments/assets/a7da7b58-4b57-40b4-9945-34ffd22707f5)

