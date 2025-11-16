from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import json
import logging
import os

# ===== CONFIGURATION =====
PARQUET_DIR = '/tmp/parquet_data'
CSV_PATH = '/opt/airflow/data/'
OUTPUT_PATH = '/opt/airflow/output/'

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'bus_transaction_etl',
    default_args=default_args,
    description='ETL Pipeline for Bus Transaction Data',
    schedule_interval='0 7 * * *',
    catchup=False,
    tags=['bus', 'etl', 'clickhouse'],
)

# ===== UTILITY FUNCTIONS =====
def load_to_pq(df, filename):
    """Load DataFrame to Parquet file"""
    if not os.path.exists(PARQUET_DIR):
        os.makedirs(PARQUET_DIR)
    
    table = pa.Table.from_pandas(df)
    filepath = f"{PARQUET_DIR}/{filename}.parquet"
    logging.info(f"Writing Parquet file: {filepath}")
    pq.write_table(table, filepath)


def pq_to_pandas(filename):
    """Read Parquet file into Pandas DataFrame"""
    filepath = f"{PARQUET_DIR}/{filename}.parquet"
    logging.info(f"Reading Parquet file: {filepath}")
    return pq.read_table(filepath).to_pandas()


def cleanup_parquet(filename):
    """Remove Parquet file after use"""
    filepath = f"{PARQUET_DIR}/{filename}.parquet"
    if os.path.exists(filepath):
        os.remove(filepath)
        logging.info(f"Removed Parquet file: {filepath}")


def standardize_bus_body_no(body_no):
    """Standardize bus body number to format XXX-NNN"""
    if pd.isna(body_no):
        return body_no
    
    body_no = str(body_no).replace(' ', '').replace('_', '')
    match = re.match(r'([A-Za-z]+)(\d+)([A-Za-z]*)', body_no)
    
    if match:
        letters = match.group(1).upper()
        numbers = match.group(2).zfill(3)
        suffix = match.group(3).upper() if match.group(3) else ''
        return f"{letters}-{numbers}{suffix}"
    
    return body_no


def convert_datetime_columns(df, columns):
    """Convert specified columns to datetime"""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def remove_duplicates(df, subset_col='uuid'):
    """Remove duplicates from DataFrame"""
    duplicates = df.duplicated(subset=[subset_col]).sum()
    if duplicates > 0:
        logging.info(f"Found {duplicates} duplicates, removing...")
        df = df.drop_duplicates(subset=[subset_col], keep='first')
    return df


# ===== EXTRACT FUNCTIONS =====
def extract_csv_data(**context):
    """Extract data from CSV files"""
    logging.info("Extracting CSV data...")
    
    try:
        csv_files = {
            'routes': 'dummy_routes.csv',
            'shelter': 'dummy_shelter_corridor.csv',
            'realisasi': 'dummy_realisasi_bus.csv'
        }
        
        dataframes = {}
        for key, filename in csv_files.items():
            df = pd.read_csv(f'{CSV_PATH}{filename}')
            dataframes[key] = df
            context['ti'].xcom_push(key=f'df_{key}', value=df.to_json())
            logging.info(f"Loaded {filename}: {len(df)} rows")
        
        logging.info("CSV extraction completed successfully")
        
    except Exception as e:
        logging.error(f"Error extracting CSV data: {str(e)}")
        raise


def extract_and_transform_postgres(**context):
    """Extract from PostgreSQL and Transform data"""
    logging.info("=" * 60)
    logging.info("STEP 1: EXTRACT FROM POSTGRESQL")
    logging.info("=" * 60)
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_tj')
        
        # ===== PERUBAHAN DIMULAI: Menggunakan SELECT eksplisit =====
        # Mengganti SELECT * untuk memastikan skema stabil
        sql_bus = """
            SELECT uuid, waktu_transaksi, no_body_var, status_var, 
                   fare_int, card_type_var, gate_in_boo 
            FROM dummy_transaksi_bus
        """
        sql_halte = """
            SELECT uuid, waktu_transaksi, status_var, fare_int, 
                   card_type_var, gate_in_boo 
            FROM dummy_transaksi_halte
        """
        df_transaksi_bus = postgres_hook.get_pandas_df(sql_bus)
        df_transaksi_halte = postgres_hook.get_pandas_df(sql_halte)
        # ===== PERUBAHAN SELESAI =====
        
        logging.info(f"Extracted - Bus: {len(df_transaksi_bus)}, Halte: {len(df_transaksi_halte)} rows")
        
    except Exception as e:
        logging.error(f"Error extracting PostgreSQL data: {str(e)}")
        raise
    
    # ===== STEP 2: TRANSFORM =====
    logging.info("=" * 60)
    logging.info("STEP 2: TRANSFORM DATA")
    logging.info("=" * 60)
    
    try:
        ti = context['ti']
        
        # Retrieve CSV data
        df_routes = pd.read_json(ti.xcom_pull(key='df_routes', task_ids='extract_csv'))
        df_shelter = pd.read_json(ti.xcom_pull(key='df_shelter', task_ids='extract_csv'))
        df_realisasi = pd.read_json(ti.xcom_pull(key='df_realisasi', task_ids='extract_csv'))
        
        # Convert datetime columns
        df_realisasi = convert_datetime_columns(df_realisasi, ['tanggal_realisasi'])
        df_transaksi_bus = convert_datetime_columns(df_transaksi_bus, ['waktu_transaksi'])
        df_transaksi_halte = convert_datetime_columns(df_transaksi_halte, ['waktu_transaksi'])
        
        # Remove duplicates
        df_transaksi_bus = remove_duplicates(df_transaksi_bus)
        df_transaksi_halte = remove_duplicates(df_transaksi_halte)
        
        # Standardize bus body numbers
        logging.info("Standardizing bus body numbers...")
        df_realisasi['bus_body_no'] = df_realisasi['bus_body_no'].apply(standardize_bus_body_no)
        df_transaksi_bus['no_body_var'] = df_transaksi_bus['no_body_var'].apply(standardize_bus_body_no)
        
        # Filter customers (status_var = 'S')
        df_transaksi_bus = df_transaksi_bus[df_transaksi_bus['status_var'] == 'S'].copy()
        df_transaksi_halte = df_transaksi_halte[df_transaksi_halte['status_var'] == 'S'].copy()
        
        logging.info(f"Customer transactions - Bus: {len(df_transaksi_bus)}, Halte: {len(df_transaksi_halte)}")

        # ===== PERUBAHAN DIMULAI: Menyimpan data bersih ke Parquet =====
        logging.info("=" * 60)
        logging.info("STEP 2.5: SAVING CLEANED TRANSACTIONS TO PARQUET")
        logging.info("=" * 60)
        
        load_to_pq(df_transaksi_bus, "cleaned_transaksi_bus")
        logging.info(f"Saved cleaned_transaksi_bus: {len(df_transaksi_bus)} rows")
        
        load_to_pq(df_transaksi_halte, "cleaned_transaksi_halte")
        logging.info(f"Saved cleaned_transaksi_halte: {len(df_transaksi_halte)} rows")
        # ===== PERUBAHAN SELESAI =====
        
    except Exception as e:
        logging.error(f"Error during transformation: {str(e)}")
        raise
    
    # ===== STEP 3: AGGREGATE =====
    logging.info("=" * 60)
    logging.info("STEP 3: AGGREGATE DATA")
    logging.info("=" * 60)
    
    try:
        # Combine transactions
        df_all_transactions = pd.concat([df_transaksi_bus, df_transaksi_halte], ignore_index=True)
        df_all_transactions['tanggal'] = df_all_transactions['waktu_transaksi'].dt.date
        
        # Define aggregation configs
        aggregations = [
            {
                'name': 'card_type',
                'group_by': ['tanggal', 'card_type_var', 'gate_in_boo'],
                'columns': ['tanggal', 'card_type', 'gate_in', 'jumlah_pelanggan', 'total_amount'],
                'df': df_all_transactions
            },
            {
                'name': 'fare',
                'group_by': ['tanggal', 'fare_int', 'gate_in_boo'],
                'columns': ['tanggal', 'tarif', 'gate_in', 'jumlah_pelanggan', 'total_amount'],
                'df': df_all_transactions
            }
        ]
        
        # Perform aggregations
        for agg_config in aggregations:
            df_agg = agg_config['df'].groupby(agg_config['group_by']).agg(
                jumlah_pelanggan=('uuid', 'count'),
                total_amount=('fare_int', 'sum')
            ).reset_index()
            
            df_agg.columns = agg_config['columns']
            load_to_pq(df_agg, f"agg_{agg_config['name']}")
            logging.info(f"Aggregated by {agg_config['name']}: {len(df_agg)} rows")
        
        # Route aggregation (needs merge)
        df_with_route = df_all_transactions.merge(
            df_realisasi[['bus_body_no', 'rute_realisasi', 'tanggal_realisasi']],
            left_on='no_body_var',
            right_on='bus_body_no',
            how='left'
        ).merge(
            df_routes[['route_code', 'route_name']],
            left_on='rute_realisasi',
            right_on='route_code',
            how='left'
        )
        
        agg_route = df_with_route.groupby(
            ['tanggal', 'route_code', 'route_name', 'gate_in_boo']
        ).agg(
            jumlah_pelanggan=('uuid', 'count'),
            total_amount=('fare_int', 'sum')
        ).reset_index()
        
        agg_route.columns = ['tanggal', 'route_code', 'route_name', 'gate_in', 'jumlah_pelanggan', 'total_amount']
        load_to_pq(agg_route, 'agg_route')
        logging.info(f"Aggregated by route: {len(agg_route)} rows")
        
        logging.info("All aggregations completed successfully")
        
    except Exception as e:
        logging.error(f"Error during aggregation: {str(e)}")
        raise


# ===== LOAD FUNCTIONS =====
def load_to_postgres(**context):
    """Load aggregated data to PostgreSQL"""
    logging.info("=" * 60)
    logging.info("LOADING TO POSTGRESQL")
    logging.info("=" * 60)
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_tj')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Load each aggregation
        agg_tables = ['card_type', 'route', 'fare']
        
        for table in agg_tables:
            df = pq_to_pandas(f'agg_{table}')
            df.to_sql(f'agg_by_{table}', engine, if_exists='replace', index=False)
            logging.info(f"Loaded agg_by_{table}: {len(df)} rows")
            
        # ===== PERUBAHAN DIMULAI: Load data bersih ke Postgres =====
        cleaned_tables_map = {
            'cleaned_transaksi_bus': 'transaksi_bus_cleaned',
            'cleaned_transaksi_halte': 'transaksi_halte_cleaned'
        }
        
        for pq_file, pg_table in cleaned_tables_map.items():
            df = pq_to_pandas(pq_file)
            df.to_sql(pg_table, engine, if_exists='replace', index=False)
            logging.info(f"Loaded {pg_table}: {len(df)} rows")
        # ===== PERUBAHAN SELESAI =====
            
        logging.info("PostgreSQL load completed successfully")
        
    except Exception as e:
        logging.error(f"Error loading to PostgreSQL: {str(e)}")
        raise


def load_to_clickhouse(**context):
    """Load aggregated data to ClickHouse"""
    logging.info("=" * 60)
    logging.info("LOADING TO CLICKHOUSE")
    logging.info("=" * 60)
    
    try:
        from clickhouse_driver import Client
        
        # Get ClickHouse connection
        ch_conn = BaseHook.get_connection("clickhouse-conn")
        extras = json.loads(ch_conn.extra or '{}')
        
        client = Client(
            host=ch_conn.host,
            port=ch_conn.port,
            user=extras.get("user", ch_conn.login),
            password=extras.get("password", ch_conn.password),
            database=extras.get("database", ch_conn.schema),
        )
        
        # Table definitions
        table_definitions = {
            'agg_by_card_type': '''
                CREATE TABLE IF NOT EXISTS agg_by_card_type (
                    tanggal Date,
                    card_type String,
                    gate_in UInt8,
                    jumlah_pelanggan UInt32,
                    total_amount Int64
                ) ENGINE = MergeTree()
                ORDER BY (tanggal, card_type, gate_in)
            ''',
            'agg_by_route': '''
                CREATE TABLE IF NOT EXISTS agg_by_route (
                    tanggal Date,
                    route_code String,
                    route_name String,
                    gate_in UInt8,
                    jumlah_pelanggan UInt32,
                    total_amount Int64
                ) ENGINE = MergeTree()
                ORDER BY (tanggal, route_code, gate_in)
            ''',
            'agg_by_fare': '''
                CREATE TABLE IF NOT EXISTS agg_by_fare (
                    tanggal Date,
                    tarif Int32,
                    gate_in UInt8,
                    jumlah_pelanggan UInt32,
                    total_amount Int64
                ) ENGINE = MergeTree()
                ORDER BY (tanggal, tarif, gate_in)
            ''',
            # ===== PERUBAHAN DIMULAI: Definisi tabel bersih =====
            'transaksi_bus_cleaned': '''
                CREATE TABLE IF NOT EXISTS transaksi_bus_cleaned (
                    uuid String,
                    waktu_transaksi DateTime,
                    no_body_var String,
                    status_var String,
                    fare_int Int32,
                    card_type_var String,
                    gate_in_boo UInt8
                ) ENGINE = MergeTree()
                ORDER BY (waktu_transaksi, no_body_var)
            ''',
            'transaksi_halte_cleaned': '''
                CREATE TABLE IF NOT EXISTS transaksi_halte_cleaned (
                    uuid String,
                    waktu_transaksi DateTime,
                    status_var String,
                    fare_int Int32,
                    card_type_var String,
                    gate_in_boo UInt8
                ) ENGINE = MergeTree()
                ORDER BY (waktu_transaksi, uuid)
            '''
            # ===== PERUBAHAN SELESAI =====
        }
        
        # Create tables and load data
        
        # ===== PERUBAHAN DIMULAI: Menggabungkan semua file untuk dimuat & dihapus =====
        all_files_to_load_map = {
            # Agregasi
            'agg_card_type': 'agg_by_card_type',
            'agg_route': 'agg_by_route',
            'agg_fare': 'agg_by_fare',
            # Data Bersih
            'cleaned_transaksi_bus': 'transaksi_bus_cleaned',
            'cleaned_transaksi_halte': 'transaksi_halte_cleaned'
        }
        
        for pq_file, ch_table in all_files_to_load_map.items():
            
            # Create table
            client.execute(table_definitions[ch_table])
            logging.info(f"Table {ch_table} ready")
            
            # Load data
            df = pq_to_pandas(pq_file)
            client.execute(f'TRUNCATE TABLE {ch_table}')
            client.execute(f'INSERT INTO {ch_table} VALUES', df.to_dict('records'))
            logging.info(f"Loaded {ch_table}: {len(df)} rows")
            
            # Cleanup parquet file
            cleanup_parquet(pq_file)
        # ===== PERUBAHAN SELESAI =====
            
        logging.info("ClickHouse load completed successfully")
        
    except Exception as e:
        logging.error(f"Error loading to ClickHouse: {str(e)}")
        raise


# ===== DEFINE TASKS =====
extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv_data,
    dag=dag,
)

extract_transform_task = PythonOperator(
    task_id='extract_transform_postgres',
    python_callable=extract_and_transform_postgres,
    dag=dag,
)

load_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

load_clickhouse_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag,
)

# ===== TASK DEPENDENCIES =====
extract_csv_task >> extract_transform_task >> [load_postgres_task, load_clickhouse_task]