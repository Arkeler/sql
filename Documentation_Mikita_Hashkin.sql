--01_Table_Creation_Mikita_Hashkin
CREATE TABLE sales_data (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    product_id INT NOT NULL,
    region_id INT NOT NULL,
    salesperson_id INT NOT NULL,
    sale_amount NUMERIC NOT NULL
) PARTITION BY RANGE (sale_date);

DO $$ 
DECLARE
    partition_date DATE;
BEGIN
    FOR i IN 0..12 LOOP
        partition_date := (CURRENT_DATE - INTERVAL '1 month' * i)::DATE;
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS sales_data_%s PARTITION OF sales_data
            FOR VALUES FROM (%L) TO (%L);',
            to_char(partition_date, 'YYYY_MM'),
            to_char(partition_date, 'YYYY-MM-01'),
            to_char(partition_date + INTERVAL '1 month', 'YYYY-MM-01')
        );
    END LOOP;
END $$;

--02_Data_Insertion_Mikita_Hashkin
DO $$
DECLARE
    start_date DATE := (CURRENT_DATE - INTERVAL '1 year')::DATE;
    end_date DATE := CURRENT_DATE;
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO sales_data (sale_date, product_id, region_id, salesperson_id, sale_amount)
        VALUES (
            (start_date + (RANDOM() * (end_date - start_date + 1))::INT)::DATE,
            (i % 10) + 1,
            (i % 5) + 1,
            (i % 20) + 1,
            (RANDOM() * 1000)::NUMERIC
        );
    END LOOP;
END $$;

--03_Query_Partitions_Mikita_Hashkin

SELECT * FROM sales_data
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31';

SELECT to_char(sale_date, 'YYYY-MM') AS month, SUM(sale_amount) AS total_sales
FROM sales_data
GROUP BY month
ORDER BY month;

SELECT salesperson_id, SUM(sale_amount) AS total_sales
FROM sales_data
WHERE region_id = 1 
GROUP BY salesperson_id
ORDER BY total_sales DESC
LIMIT 3;

--04_Maintenance_Task_Mikita_Hashkin

DO $$ 
DECLARE
    old_partition_name TEXT;
    new_partition_date DATE;
    partition_exists BOOLEAN;
BEGIN
    FOR i IN 12..23 LOOP
        old_partition_name := to_char((CURRENT_DATE - INTERVAL '1 month' * i)::DATE, 'YYYY_MM');
        EXECUTE format('DROP TABLE IF EXISTS sales_data_%s;', old_partition_name);
    END LOOP;

    new_partition_date := (CURRENT_DATE + INTERVAL '1 month')::DATE;
    partition_exists := EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = format('sales_data_%s', to_char(new_partition_date, 'YYYY_MM'))
    );
    IF NOT partition_exists THEN
        EXECUTE format('
            CREATE TABLE sales_data_%s PARTITION OF sales_data
            FOR VALUES FROM (%L) TO (%L);',
            to_char(new_partition_date, 'YYYY_MM'),
            to_char(new_partition_date, 'YYYY-MM-01'),
            to_char(new_partition_date + INTERVAL '1 month', 'YYYY-MM-01')
        );
    END IF;
END $$;

SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE 'sales_data_%'
ORDER BY table_name;

