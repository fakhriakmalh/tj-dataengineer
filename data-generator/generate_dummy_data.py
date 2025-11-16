import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Create output directory
OUTPUT_DIR = 'dummy_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 60)
print("Bus Transaction Dummy Data Generator")
print("=" * 60)

# Helper functions
def random_date(start, end):
    """Generate random date between start and end"""
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def random_element(arr):
    """Get random element from array"""
    return random.choice(arr)

def format_date(date):
    """Format date to YYYY-MM-DD"""
    return date.strftime('%Y-%m-%d')

def format_datetime(date):
    """Format datetime to YYYY-MM-DD HH:MM:SS"""
    return date.strftime('%Y-%m-%d %H:%M:%S')

# ===== 1. DUMMY_ROUTES (10 rows) =====
print("\n[1/5] Generating dummy_routes...")

route_codes = ['KRW', 'SMB', 'LGS', 'BRT', 'TJK', 'BKS', 'DPK', 'TNJ', 'PSR', 'CKR']
route_names = [
    'Karawaci - Bundaran Senayan',
    'Sumber Batu - Harmoni',
    'Lebak Bulus - Grogol',
    'Blok M - Ragunan - Tanah Abang',
    'Tangerang - Jakarta Kota',
    'Bekasi - Cempaka Putih',
    'Depok - Plaza Senayan',
    'Tanjung Priok - Senen',
    'Pasar Minggu - Kota',
    'Cikarang - Rawamangun'
]

df_routes = pd.DataFrame({
    'route_code': route_codes,
    'route_name': route_names
})

# Save to CSV
df_routes.to_csv(f'{OUTPUT_DIR}/dummy_routes.csv', index=False)
print(f"✓ Created: dummy_routes.csv ({len(df_routes)} rows)")

# ===== 2. DUMMY_SHELTER_CORRIDOR (50 rows) =====
print("\n[2/5] Generating dummy_shelter_corridor...")

shelter_prefixes = ['Halte', 'Shelter', 'Terminal']
locations = [
    'Bundaran HI', 'Senayan', 'Harmoni', 'Kota', 'Blok M', 'Lebak Bulus',
    'Ragunan', 'Kampung Melayu', 'Pulogadung', 'Cawang', 'Dukuh Atas',
    'Monas', 'Sarinah', 'Tosari', 'CSW', 'Matraman', 'Jatinegara',
    'Cempaka Putih', 'Senen', 'Tanjung Priok', 'Ancol', 'Pluit',
    'Grogol', 'Kalideres', 'Pesing'
]

shelters = []
shelter_count = 0

for code in route_codes:
    num_shelters = random.randint(3, 5)  # 3-5 shelters per corridor
    for i in range(num_shelters):
        if shelter_count >= 50:
            break
        shelters.append({
            'shelter_name_var': f"{random_element(shelter_prefixes)} {random_element(locations)} {i + 1}",
            'corridor_code': code,
            'corridor_name': route_names[route_codes.index(code)]
        })
        shelter_count += 1

df_shelters = pd.DataFrame(shelters)

# Save to CSV
df_shelters.to_csv(f'{OUTPUT_DIR}/dummy_shelter_corridor.csv', index=False)
print(f"✓ Created: dummy_shelter_corridor.csv ({len(df_shelters)} rows)")

# ===== 3. DUMMY_REALISASI_BUS (80 rows) =====
print("\n[3/5] Generating dummy_realisasi_bus...")

bus_body_nos = []
for i in range(80):
    prefix = random_element(['BRT', 'LGS', 'KRW', 'SMB', 'TJK'])
    num = random.randint(1, 500)
    suffix = random_element(['A', 'B', '']) if random.random() > 0.7 else ''
    
    # Generate various formats (will need standardization)
    formats = [
        f"{prefix} {num}{suffix}",      # e.g., BRT 15
        f"{prefix}_{num}{suffix}",      # e.g., LGS_251A
        f"{prefix}{num}{suffix}"        # e.g., KRW123
    ]
    bus_body_nos.append(random_element(formats))

start_date = datetime(2025, 11, 1)
end_date = datetime(2025, 11, 15)

df_realisasi = pd.DataFrame({
    'tanggal_realisasi': [format_date(random_date(start_date, end_date)) for _ in range(80)],
    'bus_body_no': bus_body_nos,
    'rute_realisasi': [random_element(route_codes) for _ in range(80)]
})

# Save to CSV
df_realisasi.to_csv(f'{OUTPUT_DIR}/dummy_realisasi_bus.csv', index=False)
print(f"✓ Created: dummy_realisasi_bus.csv ({len(df_realisasi)} rows)")

# ===== 4. DUMMY_TRANSAKSI_BUS (100 rows) =====
print("\n[4/5] Generating dummy_transaksi_bus...")

card_types = ['Kartu Reguler', 'Kartu Pelajar', 'Kartu Lansia', 'Kartu Disabilitas']
status_types = ['S', 'F', 'P']  # S=Success, F=Failed, P=Pending
fares = [3500, 5000, 7000, 10000]

transaksi_bus = []
for i in range(100):
    trans_date = random_date(start_date, end_date)
    fare = random_element(fares)
    balance_before = random.randint(10000, 60000)
    
    transaksi_bus.append({
        'uuid': f"bus-{i + 1}-{int(datetime.now().timestamp())}-{random.randint(100000, 999999)}",
        'waktu_transaksi': format_datetime(trans_date),
        'armada_id_var': f"ARM-{random.randint(1, 50)}",
        'no_body_var': random_element(bus_body_nos),
        'card_number_var': f"6282{random.randint(1000000000, 9999999999)}",
        'card_type_var': random_element(card_types),
        'balance_before_int': balance_before,
        'fare_int': fare,
        'balance_after_int': balance_before - fare,
        'transcode_txt': f"TRX{random.randint(100000, 999999)}",
        'gate_in_boo': random.choice([0, 1]),
        'p_latitude_flo': round(-6.2 + random.random() * 0.4, 6),
        'p_longitude_flo': round(106.7 + random.random() * 0.4, 6),
        'status_var': random_element(status_types),
        'free_service_boo': 1 if random.random() > 0.9 else 0,
        'insert_on_dtm': format_datetime(datetime.now())
    })

df_transaksi_bus = pd.DataFrame(transaksi_bus)

# Save to CSV
df_transaksi_bus.to_csv(f'{OUTPUT_DIR}/dummy_transaksi_bus.csv', index=False)
print(f"✓ Created: dummy_transaksi_bus.csv ({len(df_transaksi_bus)} rows)")

# ===== 5. DUMMY_TRANSAKSI_HALTE (100 rows) =====
print("\n[5/5] Generating dummy_transaksi_halte...")

transaksi_halte = []
for i in range(100):
    trans_date = random_date(start_date, end_date)
    fare = random_element(fares)
    balance_before = random.randint(10000, 60000)
    
    transaksi_halte.append({
        'uuid': f"halte-{i + 1}-{int(datetime.now().timestamp())}-{random.randint(100000, 999999)}",
        'waktu_transaksi': format_datetime(trans_date),
        'shelter_name_var': random_element(shelters)['shelter_name_var'],
        'terminal_name_var': f"Terminal {random_element(['A', 'B', 'C', 'D'])}",
        'card_number_var': f"6282{random.randint(1000000000, 9999999999)}",
        'card_type_var': random_element(card_types),
        'balance_before_int': balance_before,
        'fare_int': fare,
        'balance_after_int': balance_before - fare,
        'transcode_txt': f"TRX{random.randint(100000, 999999)}",
        'gate_in_boo': random.choice([0, 1]),
        'p_latitude_flo': round(-6.2 + random.random() * 0.4, 6),
        'p_longitude_flo': round(106.7 + random.random() * 0.4, 6),
        'status_var': random_element(status_types),
        'free_service_boo': 1 if random.random() > 0.9 else 0,
        'insert_on_dtm': format_datetime(datetime.now())
    })

df_transaksi_halte = pd.DataFrame(transaksi_halte)

# Save to CSV
df_transaksi_halte.to_csv(f'{OUTPUT_DIR}/dummy_transaksi_halte.csv', index=False)
print(f"✓ Created: dummy_transaksi_halte.csv ({len(df_transaksi_halte)} rows)")

# ===== Summary =====
print("\n" + "=" * 60)
print("✓ DATA GENERATION COMPLETE!")
print("=" * 60)
print(f"\nAll CSV files saved to: {OUTPUT_DIR}/")
print("\nSummary:")
print(f"  • dummy_routes.csv              : {len(df_routes):>3} rows")
print(f"  • dummy_shelter_corridor.csv    : {len(df_shelters):>3} rows")
print(f"  • dummy_realisasi_bus.csv       : {len(df_realisasi):>3} rows")
print(f"  • dummy_transaksi_bus.csv       : {len(df_transaksi_bus):>3} rows")
print(f"  • dummy_transaksi_halte.csv     : {len(df_transaksi_halte):>3} rows")
print(f"\nTotal: {len(df_routes) + len(df_shelters) + len(df_realisasi) + len(df_transaksi_bus) + len(df_transaksi_halte)} rows generated")
print("\n💡 Note: Bus body numbers include various formats (BRT 15, LGS_251A, etc.)")
print("   that need standardization in the ETL pipeline.")
print("=" * 60)