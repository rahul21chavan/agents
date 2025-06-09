
-- 📘 SCHEMA: Sample Retail Database

-- Customers Table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    gender CHAR(1),
    age INT,
    signup_date DATE
);

-- Orders Table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Order Items Table
CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Products Table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    brand VARCHAR(50)
);

-- Returns Table
CREATE TABLE returns (
    return_id INT PRIMARY KEY,
    order_id INT,
    return_date DATE,
    reason VARCHAR(255),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Campaigns Table
CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(100),
    start_date DATE,
    end_date DATE
);

-- Campaign Clicks Table
CREATE TABLE campaign_clicks (
    click_id INT PRIMARY KEY,
    campaign_id INT,
    customer_id INT,
    click_date DATE,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);



-- 🔹 MOCK DATA INSERTS

-- Customers
INSERT INTO customers VALUES (1, 'Alice', 'F', 29, '2023-01-10');
INSERT INTO customers VALUES (2, 'Bob', 'M', 34, '2023-02-14');
INSERT INTO customers VALUES (3, 'Carol', 'F', 25, '2023-03-20');

-- Products
INSERT INTO products VALUES (101, 'Laptop', 'Electronics', 1200.00, 'Dell');
INSERT INTO products VALUES (102, 'Smartphone', 'Electronics', 800.00, 'Samsung');
INSERT INTO products VALUES (103, 'Backpack', 'Accessories', 60.00, 'Nike');

-- Orders
INSERT INTO orders VALUES (1001, 1, '2023-02-01', 'Shipped');
INSERT INTO orders VALUES (1002, 2, '2023-02-15', 'Returned');
INSERT INTO orders VALUES (1003, 3, '2023-03-01', 'Delivered');

-- Order Items
INSERT INTO order_items VALUES (201, 1001, 101, 1, 1200.00);
INSERT INTO order_items VALUES (202, 1002, 102, 1, 800.00);
INSERT INTO order_items VALUES (203, 1003, 103, 2, 120.00);

-- Returns
INSERT INTO returns VALUES (301, 1002, '2023-02-20', 'Damaged');

-- Campaigns
INSERT INTO campaigns VALUES (401, 'Winter Sale', '2023-01-01', '2023-02-01');

-- Campaign Clicks
INSERT INTO campaign_clicks VALUES (501, 401, 1, '2023-01-10');
INSERT INTO campaign_clicks VALUES (502, 401, 2, '2023-01-15');



-- 🧱 STARTER CTE TEMPLATE

-- Goal: Monthly Revenue + Returns per Category

WITH order_details AS (
    SELECT
        o.order_id,
        o.order_date,
        p.category,
        oi.quantity,
        oi.price,
        (oi.quantity * oi.price) AS total
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
),

returns_flagged AS (
    SELECT DISTINCT order_id FROM returns
),

revenue_summary AS (
    SELECT
        category,
        DATE_TRUNC('month', order_date) AS month,
        SUM(total) AS gross_revenue,
        COUNT(DISTINCT CASE WHEN r.order_id IS NOT NULL THEN o.order_id END) AS returned_orders
    FROM order_details o
    LEFT JOIN returns_flagged r ON o.order_id = r.order_id
    GROUP BY category, DATE_TRUNC('month', order_date)
)

SELECT * FROM revenue_summary
ORDER BY category, month;
