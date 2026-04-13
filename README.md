# 🛒 Sales ETL Pipeline Project

An end-to-end data engineering project that extracts raw sales CSV data, transforms and cleans it using **Python & Pandas**, loads it into a **MySQL** star-schema database with incremental logic, and visualises business insights in **Power BI**.

---

## 📁 Project Structure

```
SALES_ETL_PROJECT/
│
├── dashboard/
│   └── sales_dashboard.pbix       # Power BI dashboard file
│
├── data/
│   └── samplesuperstore.csv       # Raw source data
│
├── scripts/
│   └── etl_pipeline.py            # Main ETL pipeline script
│
└── sql/
    ├── table_creation.sql         # MySQL schema (CREATE TABLE statements)
    └── analytics_queries.sql      # Business analysis SQL queries
```

---

## 🔧 Tools & Technologies

| Layer | Tool |
|---|---|
| Language | Python 3 |
| Data transformation | Pandas |
| Database | MySQL |
| ORM / DB connector | SQLAlchemy + PyMySQL |
| Visualisation | Power BI |

---

## ✨ Features

- **Data cleaning** — column name standardisation, type casting, null and duplicate handling
- **Data modelling** — star schema with `customers`, `products`, and `orders` tables
- **Primary / Foreign keys** — referential integrity enforced across all three tables
- **Incremental load** — only new records not already in the database are inserted on each run
- **SQL analytics** — business queries for sales, profit, and customer insights
- **Power BI dashboard** — dark-themed interactive dashboard with slicers, KPI cards, and charts

---

## 🗄️ Database Schema

```sql
CREATE TABLE customers (
    customer_id    VARCHAR(50)  PRIMARY KEY,
    customer_name  VARCHAR(100),
    segment        VARCHAR(50),
    country_region VARCHAR(50),
    city           VARCHAR(50),
    state_province VARCHAR(50),
    postal_code    VARCHAR(20),
    region         VARCHAR(50)
);

CREATE TABLE products (
    product_id   VARCHAR(50)  PRIMARY KEY,
    category     VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(200)
);

CREATE TABLE orders (
    order_id    VARCHAR(50),
    order_date  DATE,
    ship_date   DATE,
    ship_mode   VARCHAR(50),
    customer_id VARCHAR(50),
    product_id  VARCHAR(50),
    sales       DECIMAL(10,2),
    quantity    INT,
    discount    DECIMAL(5,2),
    profit      DECIMAL(10,2),
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id)  REFERENCES products(product_id)
);

ALTER TABLE customers ADD UNIQUE(customer_id);
ALTER TABLE products  ADD UNIQUE(product_id);
```

### Star Schema

```
dim_customers              dim_products
─────────────              ────────────
customer_id (PK)           product_id (PK)
customer_name              product_name
segment                    category
country_region             sub_category
city
state_province                       fact_orders
postal_code                          ───────────
region                               order_id    (PK)
    │                                product_id  (PK, FK) ──→ dim_products
    └────────────────────────────── customer_id  (FK)    ──→ dim_customers
                                     order_date
                                     ship_date
                                     ship_mode
                                     sales
                                     quantity
                                     discount
                                     profit
```

---

## ⚙️ ETL Pipeline Workflow

```
EXTRACT          TRANSFORM             DATA QUALITY        TABLE SPLIT
────────         ──────────            ────────────        ───────────
Read CSV    →    Rename columns   →    Drop duplicates →   df_customers
(samplesup       Lowercase/snake        Fill nulls          df_products
 erstore.csv)    Fix date types         (postal_code=0      df_orders
                 Cast sales/profit/     discount=0)
                 quantity types
                        │
                        ▼
              INCREMENTAL FILTER        LOAD
              ─────────────────         ────
              Compare against      →    Append only
              existing DB IDs           new rows to
              (customers,               MySQL via
               products, orders)        SQLAlchemy
```

### Pipeline Console Output

```
EXTRACT STAGE COMPLETED
TRANSFORM STAGE COMPLETED
Duplicates : 0
Null values : 0
DATA QUALITY COMPLETED
TABLE SPLIT COMPLETED
INCREMENTAL FILTER COMPLETED
New customers : 12
New products  : 48
New orders    : 320
LOAD COMPLETED
ETL PIPELINE COMPLETED
```

---

## 🐍 Key Code — `scripts/etl_pipeline.py`

### Extract
```python
df_super_store_sales = pd.read_csv(
    r'data/samplesuperstore.csv'
)
```

### Transform
```python
# Standardise column names
df_super_store_sales.columns = (
    df_super_store_sales.columns
    .str.lower()
    .str.replace(' ', '_', regex=False)
    .str.replace('-', '_', regex=False)
    .str.replace('/', '_', regex=False)
)

# Fix date types
df_super_store_sales['order_date'] = pd.to_datetime(df_super_store_sales['order_date'], errors='coerce')
df_super_store_sales['ship_date']  = pd.to_datetime(df_super_store_sales['ship_date'],  errors='coerce')

# Fix numeric types
df_super_store_sales['sales']    = df_super_store_sales['sales'].astype(float)
df_super_store_sales['profit']   = df_super_store_sales['profit'].astype(float)
df_super_store_sales['quantity'] = df_super_store_sales['quantity'].astype(int)
```

### Incremental Load
```python
# Read existing IDs from DB
db_orders = pd.read_sql("SELECT order_id, product_id FROM orders", engine)

# Keep only rows not already loaded
df_orders = df_orders.merge(
    db_orders,
    on=['order_id', 'product_id'],
    how='left',
    indicator=True
)
df_orders = df_orders[df_orders['_merge'] == 'left_only'].drop(columns=['_merge'])
```

### Load
```python
df_customers.to_sql('customers', engine, if_exists='append', index=False, method='multi')
df_products.to_sql('products',   engine, if_exists='append', index=False, method='multi')
df_orders.to_sql('orders',       engine, if_exists='append', index=False, method='multi')
```

---

## 📊 Sample Analytics Queries — `sql/analytics_queries.sql`

```sql
-- Monthly sales trend
SELECT MONTH(order_date) AS month,
       ROUND(SUM(sales), 2) AS monthly_sales
FROM orders
GROUP BY MONTH(order_date)
ORDER BY month;

-- Sales and profit by category
SELECT p.category,
       ROUND(SUM(o.sales), 2)  AS total_sales,
       ROUND(SUM(o.profit), 2) AS total_profit,
       ROUND(SUM(o.profit) / SUM(o.sales) * 100, 1) AS profit_margin_pct
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

-- Top customers by sales
SELECT c.customer_name, c.segment, c.region,
       ROUND(SUM(o.sales), 2)          AS total_sales,
       ROUND(SUM(o.profit), 2)         AS total_profit,
       COUNT(DISTINCT o.order_id)      AS total_orders
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.segment, c.region
ORDER BY total_sales DESC
LIMIT 10;

-- Average profit margin
SELECT ROUND(SUM(profit) / SUM(sales) * 100, 2) AS avg_profit_margin
FROM orders;

-- Orders by ship mode
SELECT ship_mode, COUNT(*) AS total_orders
FROM orders
GROUP BY ship_mode
ORDER BY total_orders DESC;

-- Sales by region
SELECT c.region,
       ROUND(SUM(o.sales), 2) AS total_sales
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region
ORDER BY total_sales DESC;
```

---

## 📊 Power BI Dashboard

The `sales_dashboard.pbix` connects directly to the MySQL star schema.

| Visual | Columns Used |
|---|---|
| KPI — Total Sales | `orders.sales` |
| KPI — Total Profit | `orders.profit` |
| KPI — Total Quantity | `orders.quantity` |
| KPI — Avg Discount | `orders.discount` |
| KPI — Avg Profit Margin | `SUM(profit) / SUM(sales) × 100` |
| Sales by Month (line chart) | `orders.order_date`, `orders.sales` |
| Sales by Category (bar) | `products.category`, `orders.sales` |
| Sales by Segment (donut) | `customers.segment`, `orders.sales` |
| Profit by Sub-Category (bar) | `products.sub_category`, `orders.profit` |
| Orders by Ship Mode (bar) | `orders.ship_mode` |
| Sales by Region (bar) | `customers.region`, `orders.sales` |
| Top Customers (table) | `customers.*`, `orders.sales`, `orders.profit` |
| Slicers | `segment`, `region`, `category`, `year` |

### DAX — Avg Profit Margin
```
Avg Profit Margin = DIVIDE(SUM(orders[profit]), SUM(orders[sales])) * 100
```

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/sales-etl-pipeline.git
cd sales-etl-pipeline
```

### 2. Install dependencies

```bash
pip install pandas sqlalchemy pymysql
```

### 3. Set up the MySQL database

```bash
mysql -u root -p -e "CREATE DATABASE super_store;"
mysql -u root -p super_store < sql/table_creation.sql
```

### 4. Update the DB connection string in `scripts/etl_pipeline.py`

```python
password = urllib.parse.quote("your_password")
engine   = create_engine(f"mysql+pymysql://root:{password}@127.0.0.1:3306/super_store")
```

### 5. Run the ETL pipeline

```bash
python scripts/etl_pipeline.py
```

### 6. Open the dashboard

Open `dashboard/sales_dashboard.pbix` in Power BI Desktop and update the MySQL data source to point to your local instance.

---

## 📈 Dashboard KPIs

| Metric | Value |
|---|---|
| Total Sales | ₹ 23.25 L |
| Total Quantity | 38,606 |
| Avg Discount | 15.5% |
| Total Profit | ₹ 2.92 L |
| Avg Profit Margin | 12.55% |
| Top Region | West (₹ 7.32 L) |
| Top Category | Technology (₹ 8.39 L) |
| Top Segment | Consumer (51.74%) |

---

## 📝 License

This project is for educational and portfolio purposes.

---

## 🙋 Author

Built with Python · Pandas · MySQL · SQLAlchemy · Power BI
