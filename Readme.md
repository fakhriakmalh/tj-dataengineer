# 🚍 Bus Transaction ETL Pipeline

Proyek ETL (Extract, Transform, Load) untuk analisis data transaksi bus menggunakan Apache Airflow, PostgreSQL, dan ClickHouse.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Why ClickHouse for Analytics?](#why-clickhouse-for-analytics)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Generate Dummy Data](#generate-dummy-data)
- [ETL Pipeline Explanation](#etl-pipeline-explanation)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## 🏗️ Architecture Overview

```
┌─────────────────┐
│   CSV Files     │
│   (Raw Data)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│   PostgreSQL    │──────▶│  Apache Airflow  │
│  (Transactional)│      │   (Orchestrator)  │
└─────────────────┘      └────────┬─────────┘
                                  │
                         Extract & Transform
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
         ┌──────────────────┐        ┌──────────────────┐
         │   PostgreSQL     │        │   ClickHouse     │
         │ (Reporting OLTP) │        │ (Analytics OLAP) │
         └──────────────────┘        └──────────────────┘
```

### Data Flow:
1. **Extract**: Read dari CSV files dan PostgreSQL
2. **Transform**: 
   - Deduplikasi data
   - Standarisasi format (bus body number)
   - Filter customer transactions (status = 'S')
   - Agregasi data (by card type, route, fare)
3. **Load**: 
   - PostgreSQL → Tabel agregasi untuk reporting
   - ClickHouse → Analytical queries & dashboards

---

## 🛠️ Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.9.3 | Workflow management & scheduling |
| **OLTP Database** | PostgreSQL | 16.0 | Transactional data storage |
| **OLAP Database** | ClickHouse | 25.4 | Analytical queries & aggregations |
| **Message Broker** | Redis | 7.2 | Celery task queue |
| **Language** | Python | 3.x | ETL logic & data processing |
| **Libraries** | pandas, pyarrow | - | Data manipulation & parquet I/O |

---

## 🚀 Why ClickHouse for Analytics?

### **OLTP vs OLAP Comparison**

| Aspect | PostgreSQL (OLTP) | ClickHouse (OLAP) |
|--------|-------------------|-------------------|
| **Purpose** | Transactional processing | Analytical processing |
| **Storage** | Row-oriented | **Column-oriented** ✅ |
| **Query Pattern** | Many small transactions | Few large analytical queries |
| **Write Speed** | ✅ Fast inserts/updates | Optimized for batch inserts |
| **Read Speed** | Good for specific rows | ⚡ **Extremely fast aggregations** |
| **Use Case** | CRUD operations | Reports, dashboards, analytics |
| **Compression** | ~2-3x | 🔥 **10-100x compression** |

### **Why ClickHouse for This Project?**

1. **⚡ Lightning Fast Aggregations**
   ```sql
   -- Query like this runs 10-100x faster in ClickHouse
   SELECT 
       tanggal, 
       card_type, 
       SUM(total_amount) as revenue,
       COUNT(jumlah_pelanggan) as customers
   FROM agg_by_card_type
   WHERE tanggal BETWEEN '2025-11-01' AND '2025-11-30'
   GROUP BY tanggal, card_type
   ORDER BY revenue DESC;
   ```

2. **📊 Optimized for Time-Series Data**
   - Bus transaction data adalah time-series
   - ClickHouse's MergeTree engine perfect untuk ini

3. **💾 Massive Data Compression**
   - 100GB data → 5-10GB storage
   - Lower storage costs



---

## 📦 Prerequisites

- Docker & Docker Compose installed
- Python 3.8+
- At least 4GB RAM available for Docker
- 10GB free disk space

---

## 📁 Project Structure

```
bus-transaction-etl/
├── dags/
│   └── bus_transaction_etl.py        # Airflow DAG
├── data-generator/
│   ├── generate_dummy_data.py        # Generate CSV files
│   ├── load_csv_to_postgres.py       # Initial data load
│   └── dummy_data/                   # Generated CSV files
│       ├── dummy_routes.csv
│       ├── dummy_shelter_corridor.csv
│       ├── dummy_realisasi_bus.csv
│       ├── dummy_transaksi_bus.csv
│       └── dummy_transaksi_halte.csv
├── docker-compose.yml                # Docker services configuration
├── Dockerfile                        # Custom Airflow image
├── requirements.txt                  # Python dependencies
├── logs/                            # Airflow logs
├── plugins/                         # Airflow plugins
└── README.md                        # This file
```

---

## 🚀 Setup Instructions

### **Quick Setup Checklist**

```bash
# 1. Build custom Docker image
docker build -t apache/airflow:2.9.3-custom .

# 2. Initialize Airflow
docker-compose up airflow-init

# 3. Start all services
docker-compose up -d

# 4. Access Airflow UI
open http://localhost:8080

# 5. Setup connections (see detailed steps below)
```

### **Detailed Setup Steps**

### **Step 1: Clone Repository**

```bash
git clone <your-repo-url>
cd bus-transaction-etl
```


### **Step 2: Build Custom Docker Image**

**Important**: Build the custom image BEFORE running docker-compose!

```bash
# Build the custom Airflow image
docker build -t apache/airflow:2.9.3-custom .

# Verify the image is created
docker images | grep airflow
```

**Expected Output:**
```
REPOSITORY          TAG              IMAGE ID       CREATED         SIZE
apache/airflow      2.9.3-custom     abc123def456   2 minutes ago   1.8GB
apache/airflow      2.9.3            xyz789ghi012   2 weeks ago     1.5GB
```

**What happens during build:**
1. ✅ Downloads base Airflow 2.9.3 image
2. ✅ Installs Python packages from requirements.txt
3. ✅ Installs PostgreSQL client tools
4. ✅ Creates image tagged as `apache/airflow:2.9.3-custom`

**Why custom image?**
- 📦 Pre-installed dependencies (faster container startup)
- 🔒 Consistent environment across all Airflow components
- 🚀 No need to install packages every time container starts



### **Step 3: Update docker-compose.yml**

Make sure your `docker-compose.yml` uses the custom image:

```yaml
x-airflow-common:
  &airflow-common
  # Use the custom built image
  image: apache/airflow:2.9.3-custom
  # OR if you prefer to build from Dockerfile directly:
  # build: .
  environment:
    # ... rest of configuration
```

**Two approaches:**

**Option A: Use pre-built image (Recommended)**
```yaml
image: apache/airflow:2.9.3-custom
```
- ✅ Faster startup
- ✅ Explicit version control
- ⚠️ Need to rebuild manually when updating

**Option B: Build on compose up**
```yaml
build: .
```
- ✅ Auto-rebuild on changes
- ⚠️ Slower first startup
- ⚠️ May cause confusion with image versions


### **Step 4: Start Docker Services**

```bash
# Create necessary directories
mkdir -p ./dags ./logs ./plugins ./data-generator/dummy_data

# Start services
docker-compose up -d

# Check services status
docker-compose ps
```

**Expected Output:**
```
NAME                     STATUS
airflow-init            exited (0)
airflow-scheduler       running
airflow-webserver       running
airflow-worker          running
postgres                running
postgresdb              running
clickhouse              running
redis                   running
```

### **Step 5: Access Airflow UI**

1. Open browser: http://localhost:8080
2. Login credentials:
   - Username: `airflow`
   - Password: `airflow`

**First login checklist:**
- ✅ Dashboard loads without errors
- ✅ DAGs page is accessible
- ✅ No error messages in UI
- ✅ Example DAGs are NOT visible (we disabled them)

### **Step 6: Setup Airflow Connections**

#### **PostgreSQL Connection (Bus Data)**
1. Navigate to: Admin → Connections
2. Click "+" to add new connection
3. Fill in:
   - **Conn Id**: `postgres_tj`
   - **Conn Type**: `Postgres`
   - **Host**: `host.docker.internal`
   - **Schema**: `bus_db`
   - **Login**: `postgrestj`
   - **Password**: `bustransjakarta`
   - **Port**: `5432`

#### **ClickHouse Connection**
1. Add new connection
2. Fill in:
   - **Conn Id**: `clickhouse-conn`
   - **Conn Type**: `Generic`
   - **Host**: `host.docker.internal`
   - **Port**: `9000`
   - **Extra**: 
     ```json
     {
       "user": "user",
       "password": "secretpass",
       "database": "target_db"
     }
     ```

---

## 🎲 Generate Dummy Data

### **Step 1: Generate CSV Files**

```bash
cd data-generator

# Install dependencies (if not in Docker)
pip install pandas

# Generate dummy data
python generate_dummy_data.py
```

**Output:**
```
============================================================
Bus Transaction Dummy Data Generator
============================================================

[1/5] Generating dummy_routes...
✓ Created: dummy_routes.csv (10 rows)

[2/5] Generating dummy_shelter_corridor...
✓ Created: dummy_shelter_corridor.csv (50 rows)

[3/5] Generating dummy_realisasi_bus...
✓ Created: dummy_realisasi_bus.csv (80 rows)

[4/5] Generating dummy_transaksi_bus...
✓ Created: dummy_transaksi_bus.csv (100 rows)

[5/5] Generating dummy_transaksi_halte...
✓ Created: dummy_transaksi_halte.csv (100 rows)

============================================================
✓ DATA GENERATION COMPLETE!
============================================================
```

### **Step 2: Load Initial Data to PostgreSQL**

```bash
# Update DB_CONFIG in load_csv_to_postgres.py
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,  # External port
    'database': 'bus_db',
    'user': 'postgrestj',
    'password': 'bustransjakarta'
}

# Run loader
python load_csv_to_postgres.py
```

### **Step 3: Verify Data**

```bash
# Connect to PostgreSQL
psql -h localhost -p 5433 -U postgrestj -d bus_db

# Check tables
\dt

# Check data
SELECT COUNT(*) FROM dummy_transaksi_bus;
SELECT COUNT(*) FROM dummy_transaksi_halte;
```

---

## 🔄 ETL Pipeline Explanation

### **Pipeline Overview**

```
┌─────────────────┐
│  extract_csv    │  Read routes, shelters, bus realization
└────────┬────────┘
         │
         ▼
┌──────────────────────────┐
│ extract_transform_postgres│  Extract transactions, transform, aggregate
└────────┬─────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌─────────┐ ┌──────────────┐
│ load_pg │ │ load_clickhouse│
└─────────┘ └──────────────┘
```

### **DAG Configuration**

```python
dag = DAG(
    'bus_transaction_etl',
    schedule_interval='0 7 * * *',  # Daily at 7 AM
    start_date=datetime(2025, 11, 16),
    catchup=False,
    tags=['bus', 'etl', 'clickhouse']
)
```

### **Task 1: Extract CSV Data**

**Purpose**: Load reference data (routes, shelters, bus realization)

**Input**: 
- `dummy_routes.csv`
- `dummy_shelter_corridor.csv`
- `dummy_realisasi_bus.csv`

**Output**: XCom pushed data for next tasks

**Code**:
```python
def extract_csv_data(**context):
    df_routes = pd.read_csv('dummy_routes.csv')
    df_shelter = pd.read_csv('dummy_shelter_corridor.csv')
    df_realisasi = pd.read_csv('dummy_realisasi_bus.csv')
    # Push to XCom...
```

### **Task 2: Extract & Transform PostgreSQL**

**Purpose**: Main ETL logic

**Extract Phase**:
```sql
SELECT * FROM dummy_transaksi_bus
SELECT * FROM dummy_transaksi_halte
```

**Transform Phase**:
1. **Data Cleaning**
   - Remove duplicate
   - Convert datetime columns

2. **Standardization**
   ```python
   # Before: BRT 15, LGS_251A, KRW123
   # After:  BRT-015, LGS-251A, KRW-123
   standardize_bus_body_no(body_no)
   ```

3. **Filtering**
   - Keep only successful transactions (`status_var = 'S'`)
   - Filter out failed/pending transactions

4. **Aggregation**
   
   **a) By Card Type**
   ```python
   GROUP BY tanggal, card_type_var, gate_in_boo
   AGGREGATE:
     - jumlah_pelanggan (COUNT)
     - total_amount (SUM)
   ```
   
   **b) By Route**
   ```python
   # Join with realisasi and routes
   GROUP BY tanggal, route_code, route_name, gate_in_boo
   AGGREGATE:
     - jumlah_pelanggan (COUNT)
     - total_amount (SUM)
   ```
   
   **c) By Fare**
   ```python
   GROUP BY tanggal, fare_int, gate_in_boo
   AGGREGATE:
     - jumlah_pelanggan (COUNT)
     - total_amount (SUM)
   ```

**Output**: Parquet files for loading phase

### **Task 3 & 4: Load Data**

**Load to PostgreSQL**:
- Load cleaned transaction table
- Replace mode for aggregation tables
- Used for: Operational reporting, simple queries

**Load to ClickHouse**:
- Load cleaned transaction table
- Truncate + Insert for aggregation tables
- Used for: Complex analytics, dashboards, time-series analysis


---

## 🎮 Running the Pipeline

### **Manual Trigger**

1. Go to Airflow UI: http://localhost:8080
2. Find DAG: `bus_transaction_etl`
3. Toggle DAG to **ON** (unpause)
4. Click **▶️ Trigger DAG**

### **Monitor Execution**

1. Click on DAG name
2. View **Graph** or **Grid** view
3. Click individual tasks to see logs
4. Check task duration and status

### **Scheduled Runs**

Pipeline runs automatically **every day at 7 AM**.

To change schedule:
```python
schedule_interval='0 7 * * *',  # Cron format
# Examples:
# '0 */6 * * *'  = Every 6 hours
# '0 0 * * *'    = Daily at midnight
# '0 0 * * 0'    = Weekly on Sunday
```

---

## 📊 Query Analytics Data

### **ClickHouse Queries**

```bash
# Connect to ClickHouse
docker exec -it <clickhouse_container> clickhouse-client \
  --user user \
  --password secretpass \
  --database target_db
```

**Example Queries**:

```sql
-- 1. Revenue by card type
SELECT 
    card_type,
    SUM(total_amount) as total_revenue,
    SUM(jumlah_pelanggan) as total_customers,
    AVG(total_amount / jumlah_pelanggan) as avg_per_customer
FROM agg_by_card_type
GROUP BY card_type
ORDER BY total_revenue DESC;

-- 2. Daily revenue trend
SELECT 
    tanggal,
    SUM(total_amount) as daily_revenue,
    SUM(jumlah_pelanggan) as daily_customers
FROM agg_by_card_type
GROUP BY tanggal
ORDER BY tanggal DESC
LIMIT 30;

-- 3. Top 5 routes by revenue
SELECT 
    route_code,
    route_name,
    SUM(total_amount) as total_revenue,
    SUM(jumlah_pelanggan) as total_passengers
FROM agg_by_route
GROUP BY route_code, route_name
ORDER BY total_revenue DESC
LIMIT 5;

-- 4. Gate in vs gate out comparison
SELECT 
    gate_in,
    COUNT(DISTINCT tanggal) as days,
    SUM(total_amount) as revenue,
    SUM(jumlah_pelanggan) as customers
FROM agg_by_card_type
GROUP BY gate_in;

-- 5. Fare distribution
SELECT 
    tarif,
    SUM(jumlah_pelanggan) as customers,
    SUM(total_amount) as revenue,
    ROUND(SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM agg_by_fare), 2) as pct_revenue
FROM agg_by_fare
GROUP BY tarif
ORDER BY tarif;
```

### **PostgreSQL Queries**

```bash
# Connect to PostgreSQL
psql -h localhost -p 5433 -U postgrestj -d bus_db
```

```sql
-- Check raw transaction data
SELECT 
    DATE(waktu_transaksi) as tanggal,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT card_number_var) as unique_cards,
    SUM(fare_int) as total_revenue
FROM dummy_transaksi_bus
WHERE status_var = 'S'
GROUP BY DATE(waktu_transaksi)
ORDER BY tanggal DESC;
```

---

## 🔍 Monitoring & Troubleshooting

### **Check Service Health**

```bash
# Check all services
docker-compose ps

# View logs
docker-compose logs airflow-scheduler
docker-compose logs postgres
docker-compose logs clickhouse

# Follow logs in real-time
docker-compose logs -f airflow-worker
```

---

## 📈 Next Steps

### **Enhancements**

1. **Incremental Loading**
   - Track last processed timestamp
   - Only load new/updated records
   - Reduce processing time

2. **ELT method**
   - Better for big data load
   - ELT will crash when facing amount of big data
   - using dbt (data build tool) for ELT method

3. **Partitioning**
   ```sql
   -- Partition ClickHouse tables by month
   CREATE TABLE agg_by_card_type (...)
   ENGINE = MergeTree()
   ORDER BY (tanggal, card_type, gate_in);
   ```

4. **Alerting**
   - Setup Airflow alerts (email/Slack)
   - Monitor pipeline failures
   - Track SLA violations

---

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)


---

**Happy ETL-ing! 🚀**