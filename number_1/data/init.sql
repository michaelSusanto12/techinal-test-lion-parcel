CREATE DATABASE lionparcel_source;
CREATE DATABASE lionparcel_dwh;
CREATE DATABASE airflow;

\c lionparcel_source

CREATE TABLE IF NOT EXISTS retail_transactions (
    id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_status VARCHAR(50),
    pos_origin VARCHAR(100),
    pos_destination VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_last_status ON retail_transactions(last_status);
CREATE INDEX IF NOT EXISTS idx_updated_at ON retail_transactions(updated_at);
CREATE INDEX IF NOT EXISTS idx_deleted_at ON retail_transactions(deleted_at);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_retail_transactions_updated_at ON retail_transactions;
CREATE TRIGGER update_retail_transactions_updated_at
    BEFORE UPDATE ON retail_transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

INSERT INTO retail_transactions (id, customer_id, last_status, pos_origin, pos_destination)
VALUES 
    ('LP20240100000001', 'CUST000001', 'PICKED_UP', 'Jakarta Pusat', 'Bandung'),
    ('LP20240100000002', 'CUST000002', 'IN_TRANSIT', 'Surabaya', 'Malang'),
    ('LP20240100000003', 'CUST000003', 'OUT_FOR_DELIVERY', 'Medan', 'Pekanbaru'),
    ('LP20240100000004', 'CUST000004', 'DELIVERED', 'Semarang', 'Yogyakarta'),
    ('LP20240100000005', 'CUST000005', 'DONE', 'Makassar', 'Manado')
ON CONFLICT (id) DO NOTHING;

UPDATE retail_transactions SET deleted_at = CURRENT_TIMESTAMP WHERE id = 'LP20240100000005';

\c lionparcel_dwh

CREATE TABLE IF NOT EXISTS dwh_retail_transactions (
    id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_status VARCHAR(50),
    pos_origin VARCHAR(100),
    pos_destination VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP NULL,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_dwh_last_status ON dwh_retail_transactions(last_status);
CREATE INDEX IF NOT EXISTS idx_dwh_is_deleted ON dwh_retail_transactions(is_deleted);
