-- Total sales by region
SELECT region,SUM(sales)
FROM orders o
JOIN customers c
ON o.customer_id=c.customer_id
GROUP BY region;

-- Top 10 products by profit
SELECT product_name,SUM(profit)
FROM orders o
JOIN products p
ON o.product_id=p.product_id
GROUP BY product_name
ORDER BY SUM(profit) DESC
LIMIT 10;

-- Monthly sales trend
SELECT 
YEAR(order_date),
MONTH(order_date),
SUM(sales)
FROM orders
GROUP BY 1,2;