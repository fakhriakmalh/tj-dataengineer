import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import os
from datetime import datetime

# ===== CONFIGURATION =====
DB_CONFIG = {
    'host': 'localhost',        # Ganti dengan host PostgreSQL Anda
    'port': 5432,               # Default PostgreSQL port
    'database': 'bus_db',       # Ganti dengan nama database Anda
    'user': 'airflow',         # Ganti dengan username PostgreSQL Anda
    'password': 'airflow' # Ganti dengan password PostgreSQL Anda
}

CSV_DIR = 'dummy_data'  # Folder tempat CSV files

print("=" * 70)
print("Load CSV to PostgreSQL - Bus Transaction Tables")
print("=" * 70)

# ===== CONNECT TO POSTGRESQL =====
def connect_to_postgres():
    """Create connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"\n✓ Connected to PostgreSQL database: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"\n✗ Error connecting to PostgreSQL: {str(e)}")
        print("\nPlease check your DB_CONFIG settings!")
        exit(1)

# ===== CREATE TABLES =====
def create_tables(conn):
    """Create tables if not exist"""
    print("\n[Step 1] Creating tables...")
    
    cursor = conn.cursor()
    
    # Create dummy_transaksi_bus table
    create_bus_table = """
    CREATE TABLE IF NOT EXISTS dummy_transaksi_bus (
        uuid VARCHAR(100) PRIMARY KEY,
        waktu_transaksi TIMESTAMP NOT NULL,
        armada_id_var VARCHAR(50),
        no_body_var VARCHAR(50),
        card_number_var VARCHAR(50),
        card_type_var VARCHAR(50),
        balance_before_int INTEGER,
        fare_int INTEGER,
        balance_after_int INTEGER,
        transcode_txt VARCHAR(50),
        gate_in_boo BOOLEAN,
        p_latitude_flo FLOAT,
        p_longitude_flo FLOAT,
        status_var VARCHAR(10),
        free_service_boo BOOLEAN,
        insert_on_dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create dummy_transaksi_halte table
    create_halte_table = """
    CREATE TABLE IF NOT EXISTS dummy_transaksi_halte (
        uuid VARCHAR(100) PRIMARY KEY,
        waktu_transaksi TIMESTAMP NOT NULL,
        shelter_name_var VARCHAR(100),
        terminal_name_var VARCHAR(100),
        card_number_var VARCHAR(50),
        card_type_var VARCHAR(50),
        balance_before_int INTEGER,
        fare_int INTEGER,
        balance_after_int INTEGER,
        transcode_txt VARCHAR(50),
        gate_in_boo BOOLEAN,
        p_latitude_flo FLOAT,
        p_longitude_flo FLOAT,
        status_var VARCHAR(10),
        free_service_boo BOOLEAN,
        insert_on_dtm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for performance
    create_indexes = """
    CREATE INDEX IF NOT EXISTS idx_bus_waktu ON dummy_transaksi_bus(waktu_transaksi);
    CREATE INDEX IF NOT EXISTS idx_bus_status ON dummy_transaksi_bus(status_var);
    CREATE INDEX IF NOT EXISTS idx_halte_waktu ON dummy_transaksi_halte(waktu_transaksi);
    CREATE INDEX IF NOT EXISTS idx_halte_status ON dummy_transaksi_halte(status_var);
    """
    
    try:
        cursor.execute(create_bus_table)
        print("  ✓ Table 'dummy_transaksi_bus' ready")
        
        cursor.execute(create_halte_table)
        print("  ✓ Table 'dummy_transaksi_halte' ready")
        
        cursor.execute(create_indexes)
        print("  ✓ Indexes created")
        
        conn.commit()
        cursor.close()
        
    except Exception as e:
        print(f"  ✗ Error creating tables: {str(e)}")
        conn.rollback()
        cursor.close()
        exit(1)

# ===== LOAD CSV DATA =====
def load_csv_to_table(conn, csv_file, table_name):
    """Load CSV data into PostgreSQL table"""
    print(f"\n[Step 2] Loading {csv_file}...")
    
    csv_path = os.path.join(CSV_DIR, csv_file)
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"  ✗ File not found: {csv_path}")
        return False
    
    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"  • Read {len(df)} rows from CSV")
        
        # Convert boolean columns (0/1 to True/False)
        if 'gate_in_boo' in df.columns:
            df['gate_in_boo'] = df['gate_in_boo'].astype(bool)
        if 'free_service_boo' in df.columns:
            df['free_service_boo'] = df['free_service_boo'].astype(bool)
        
        # Convert datetime columns
        if 'waktu_transaksi' in df.columns:
            df['waktu_transaksi'] = pd.to_datetime(df['waktu_transaksi'])
        if 'insert_on_dtm' in df.columns:
            df['insert_on_dtm'] = pd.to_datetime(df['insert_on_dtm'])
        
        # Get column names
        columns = df.columns.tolist()
        
        # Prepare data for insertion
        values = [tuple(row) for row in df.values]
        
        # Create INSERT query
        cursor = conn.cursor()
        
        # Use INSERT ... ON CONFLICT DO NOTHING for idempotency
        insert_query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT (uuid) DO NOTHING
        """).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Execute batch insert
        execute_values(cursor, insert_query, values)
        rows_inserted = cursor.rowcount
        
        conn.commit()
        cursor.close()
        
        print(f"  ✓ Inserted {rows_inserted} rows into '{table_name}'")
        
        if rows_inserted < len(df):
            print(f"  ℹ {len(df) - rows_inserted} rows skipped (duplicates)")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error loading data: {str(e)}")
        conn.rollback()
        return False

# ===== VERIFY DATA =====
def verify_data(conn):
    """Verify loaded data"""
    print("\n[Step 3] Verifying loaded data...")
    
    cursor = conn.cursor()
    
    try:
        # Check dummy_transaksi_bus
        cursor.execute("SELECT COUNT(*) FROM dummy_transaksi_bus")
        bus_count = cursor.fetchone()[0]
        print(f"  • dummy_transaksi_bus: {bus_count} rows")
        
        cursor.execute("""
            SELECT status_var, COUNT(*) 
            FROM dummy_transaksi_bus 
            GROUP BY status_var
        """)
        print(f"    Status breakdown:")
        for status, count in cursor.fetchall():
            print(f"      - {status}: {count}")
        
        # Check dummy_transaksi_halte
        cursor.execute("SELECT COUNT(*) FROM dummy_transaksi_halte")
        halte_count = cursor.fetchone()[0]
        print(f"\n  • dummy_transaksi_halte: {halte_count} rows")
        
        cursor.execute("""
            SELECT status_var, COUNT(*) 
            FROM dummy_transaksi_halte 
            GROUP BY status_var
        """)
        print(f"    Status breakdown:")
        for status, count in cursor.fetchall():
            print(f"      - {status}: {count}")
        
        # Show date range
        cursor.execute("""
            SELECT 
                MIN(waktu_transaksi) as earliest,
                MAX(waktu_transaksi) as latest
            FROM dummy_transaksi_bus
        """)
        earliest, latest = cursor.fetchone()
        print(f"\n  • Bus transactions date range: {earliest} to {latest}")
        
        cursor.execute("""
            SELECT 
                MIN(waktu_transaksi) as earliest,
                MAX(waktu_transaksi) as latest
            FROM dummy_transaksi_halte
        """)
        earliest, latest = cursor.fetchone()
        print(f"  • Halte transactions date range: {earliest} to {latest}")
        
        cursor.close()
        
    except Exception as e:
        print(f"  ✗ Error verifying data: {str(e)}")
        cursor.close()

# ===== MAIN EXECUTION =====
def main():
    """Main execution function"""
    
    # Connect to PostgreSQL
    conn = connect_to_postgres()
    
    # Create tables
    create_tables(conn)
    
    # Load CSV files
    success_bus = load_csv_to_table(conn, 'dummy_transaksi_bus.csv', 'dummy_transaksi_bus')
    success_halte = load_csv_to_table(conn, 'dummy_transaksi_halte.csv', 'dummy_transaksi_halte')
    
    # Verify data
    if success_bus and success_halte:
        verify_data(conn)
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 70)
    if success_bus and success_halte:
        print("✓ DATA LOADING COMPLETE!")
    else:
        print("✗ DATA LOADING FAILED - Check errors above")
    print("=" * 70)
    print("\nConnection closed.")

if __name__ == "__main__":
    main()