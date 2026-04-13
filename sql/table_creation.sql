CREATE TABLE customers (

customer_id VARCHAR(50) PRIMARY KEY,

customer_name VARCHAR(100),

segment VARCHAR(50),

country_region VARCHAR(50),

city VARCHAR(50),

state_province VARCHAR(50),

postal_code VARCHAR(20),

region VARCHAR(50)

);

CREATE TABLE products (

product_id VARCHAR(50) PRIMARY KEY,

category VARCHAR(50),

sub_category VARCHAR(50),

product_name VARCHAR(200)

);


CREATE TABLE orders (

order_id VARCHAR(50),

order_date DATE,

ship_date DATE,

ship_mode VARCHAR(50),

customer_id VARCHAR(50),

product_id VARCHAR(50),

sales DECIMAL(10,2),

quantity INT,

discount DECIMAL(5,2),

profit DECIMAL(10,2),

PRIMARY KEY(order_id,product_id),

FOREIGN KEY (customer_id)
REFERENCES customers(customer_id),

FOREIGN KEY (product_id)
REFERENCES products(product_id)

);


ALTER TABLE customers ADD UNIQUE(customer_id);
ALTER TABLE products ADD UNIQUE(product_id);