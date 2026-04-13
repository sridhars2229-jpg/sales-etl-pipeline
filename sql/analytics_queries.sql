-- ============================================
-- Q1: Total Sales, Profit and Profit Margin
-- ============================================

SELECT 
    ROUND(SUM(sales),2) AS total_sales,
    ROUND(SUM(profit),2) AS total_profit,
    ROUND(SUM(profit)/SUM(sales)*100,2) AS profit_margin_pct
FROM orders;


-- ============================================
-- Q2: Total Sales by Region
-- ============================================

SELECT 
    c.region,
    ROUND(SUM(o.sales),2) AS total_sales
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.region
ORDER BY total_sales DESC;


-- ============================================
-- Q3: Sales by Category
-- ============================================

SELECT 
    p.category,
    ROUND(SUM(o.sales),2) AS total_sales
FROM orders o
JOIN products p
ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;


-- ============================================
-- Q4: Top 10 Customers by Revenue
-- ============================================

SELECT 
    c.customer_name,
    ROUND(SUM(o.sales),2) AS total_sales
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_sales DESC
LIMIT 10;


-- ============================================
-- Q5: Top 10 Products by Profit
-- ============================================

SELECT 
    p.product_name,
    ROUND(SUM(o.profit),2) AS total_profit
FROM orders o
JOIN products p
ON o.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_profit DESC
LIMIT 10;


-- ============================================
-- Q6: Monthly Sales Trend
-- ============================================

SELECT 
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    ROUND(SUM(sales),2) AS total_sales
FROM orders
GROUP BY month
ORDER BY month;


-- ============================================
-- Q7: Profit by Region
-- ============================================

SELECT 
    c.region,
    ROUND(SUM(o.profit),2) AS total_profit
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.region
ORDER BY total_profit DESC;


-- ============================================
-- Q8: Loss Making Products
-- ============================================

SELECT 
    p.product_name,
    ROUND(SUM(o.profit),2) AS total_profit
FROM orders o
JOIN products p
ON o.product_id = p.product_id
GROUP BY p.product_name
HAVING total_profit < 0
ORDER BY total_profit;


-- ============================================
-- Q9: Top 5 Loss Making Products
-- ============================================

SELECT 
    p.product_name,
    ROUND(SUM(o.profit),2) AS total_loss
FROM orders o
JOIN products p
ON o.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_loss ASC
LIMIT 5;


-- ============================================
-- Q10: Repeat Customers
-- ============================================

SELECT 
    c.customer_name,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_name
HAVING total_orders > 1
ORDER BY total_orders DESC;


-- ============================================
-- Q11: Customer Ranking (Window Function)
-- ============================================

SELECT 
    c.customer_name,
    SUM(o.sales) AS total_sales,
    RANK() OVER (ORDER BY SUM(o.sales) DESC) AS rank_no
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_name;


-- ============================================
-- Q12: Category Contribution %
-- ============================================

SELECT 
    p.category,
    ROUND(SUM(o.sales)*100 / SUM(SUM(o.sales)) OVER (),2) AS contribution_pct
FROM orders o
JOIN products p
ON o.product_id = p.product_id
GROUP BY p.category;


-- ============================================
-- Q13: Average Shipping Days by Ship Mode
-- ============================================

SELECT 
    ship_mode,
    ROUND(AVG(DATEDIFF(ship_date, order_date)),2) AS avg_shipping_days
FROM orders
GROUP BY ship_mode;


-- ============================================
-- Q14: Top 5 States by Sales
-- ============================================

SELECT 
    c.state_province,
    ROUND(SUM(o.sales),2) AS total_sales
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.state_province
ORDER BY total_sales DESC
LIMIT 5;


-- ============================================
-- Q15: Sales Trend with Running Total
-- ============================================

SELECT 
    DATE_FORMAT(order_date,'%Y-%m') AS month,
    SUM(sales) AS monthly_sales,
    SUM(SUM(sales)) OVER (ORDER BY DATE_FORMAT(order_date,'%Y-%m')) AS running_total
FROM orders
GROUP BY month;


-- ============================================
-- Q16: Customer Segmentation
-- ============================================

SELECT 
    customer_id,
    total_sales,
    CASE 
        WHEN total_sales > 5000 THEN 'High'
        WHEN total_sales > 2000 THEN 'Medium'
        ELSE 'Low'
    END AS customer_segment
FROM (
    SELECT 
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
) t;


-- ============================================
-- Q17: Most Profitable Category
-- ============================================

SELECT 
    p.category,
    SUM(o.profit) AS total_profit
FROM orders o
JOIN products p
ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_profit DESC
LIMIT 1;


-- ============================================
-- Q18: Top Product per Category
-- ============================================

SELECT *
FROM (
    SELECT 
        p.category,
        p.product_name,
        SUM(o.sales) AS total_sales,
        RANK() OVER (PARTITION BY p.category ORDER BY SUM(o.sales) DESC) AS rank_no
    FROM orders o
    JOIN products p
    ON o.product_id = p.product_id
    GROUP BY p.category, p.product_name
) t
WHERE rank_no = 1;


-- ============================================
-- Q19: High Discount but Loss Orders
-- ============================================

SELECT *
FROM orders
WHERE discount > 0.5 AND profit < 0;


-- ============================================
-- Q20: Year-wise Sales Growth
-- ============================================

SELECT 
    YEAR(order_date) AS year,
    SUM(sales) AS total_sales,
    LAG(SUM(sales)) OVER (ORDER BY YEAR(order_date)) AS prev_year_sales,
    ROUND(
        (SUM(sales) - LAG(SUM(sales)) OVER (ORDER BY YEAR(order_date)))
        / LAG(SUM(sales)) OVER (ORDER BY YEAR(order_date)) * 100,2
    ) AS growth_pct
FROM orders
GROUP BY year;