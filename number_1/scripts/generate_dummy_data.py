"""
Generate dummy retail transaction data
"""
import random
from datetime import datetime
import psycopg2
import os


DB_CONFIG = {
    'host': os.getenv('SOURCE_DB_HOST', 'localhost'),
    'port': os.getenv('SOURCE_DB_PORT', '5436'),
    'database': os.getenv('SOURCE_DB_NAME', 'lionparcel_source'),
    'user': os.getenv('SOURCE_DB_USER', 'postgres'),
    'password': os.getenv('SOURCE_DB_PASSWORD', 'postgres')
}

STATUSES = ['PICKED_UP', 'IN_TRANSIT', 'AT_WAREHOUSE', 'OUT_FOR_DELIVERY', 'DELIVERED', 'DONE']

CITIES = [
    'Jakarta Pusat', 'Jakarta Selatan', 'Jakarta Barat', 'Jakarta Timur', 'Jakarta Utara',
    'Bandung', 'Surabaya', 'Semarang', 'Yogyakarta', 'Medan',
    'Makassar', 'Palembang', 'Denpasar', 'Malang', 'Balikpapan'
]


def generate_transaction(index: int) -> dict:
    origin = random.choice(CITIES)
    destination = random.choice([c for c in CITIES if c != origin])
    status = random.choice(STATUSES)
    
    return {
        'id': f'LP{datetime.now().strftime("%Y%m")}{index:08d}',
        'customer_id': f'CUST{random.randint(1, 5000):06d}',
        'last_status': status,
        'pos_origin': origin,
        'pos_destination': destination,
    }


def insert_transactions(num_records: int = 10000):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print(f"Generating {num_records} transactions...")
    
    insert_query = """
        INSERT INTO retail_transactions 
        (id, customer_id, last_status, pos_origin, pos_destination)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    
    batch_size = 1000
    for i in range(0, num_records, batch_size):
        batch = []
        for j in range(i, min(i + batch_size, num_records)):
            trx = generate_transaction(j)
            batch.append((
                trx['id'],
                trx['customer_id'],
                trx['last_status'],
                trx['pos_origin'],
                trx['pos_destination']
            ))
        
        cursor.executemany(insert_query, batch)
        conn.commit()
        print(f"  Inserted {min(i + batch_size, num_records)}/{num_records}")
    
    cursor.close()
    conn.close()
    print("Done!")


def simulate_soft_delete(percentage: float = 0.05):
    """Mark DONE transactions as deleted"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE retail_transactions 
        SET deleted_at = CURRENT_TIMESTAMP
        WHERE last_status = 'DONE' AND deleted_at IS NULL
        AND id IN (
            SELECT id FROM retail_transactions 
            WHERE last_status = 'DONE' AND deleted_at IS NULL
            ORDER BY RANDOM() 
            LIMIT (SELECT COUNT(*) * %s FROM retail_transactions WHERE last_status = 'DONE')::int
        )
    """, (percentage,))
    
    affected = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Soft deleted {affected} DONE records")


if __name__ == "__main__":
    insert_transactions(10000)
    simulate_soft_delete(0.5)
