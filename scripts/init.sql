
-- Create tables
CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC
);

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY,
    product_id INT,
    quantity INT,
    created_date TEXT
);

-- Optional: indexes to optimize query performance
CREATE INDEX IF NOT EXISTS idx_orders_created_date ON orders(created_date);
CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
