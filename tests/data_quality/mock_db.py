"""
Create a DuckDB database for testing the SQL-WatchPup data quality testing framework.
This script creates tables and sample data that match the configurations in the YAML files.
Tables are created WITHOUT constraints to allow testing of all data quality issues.
"""

import duckdb
import os
import random
from datetime import datetime, timedelta

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configuration
DB_FILENAME = "data_quality_test.duckdb"
DB_FILE = os.path.join(SCRIPT_DIR, DB_FILENAME)  # Create in script directory
SCHEMA_NAME = "watchpup"

print(f"Script directory: {SCRIPT_DIR}")
print(f"Database will be created at: {DB_FILE}")

# Remove existing database file if it exists
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
    print(f"Removed existing database file: {DB_FILE}")

# Connect to database
conn = duckdb.connect(DB_FILE)
print(f"Connected to DuckDB: {DB_FILE}")

# Create schema
conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
print(f"Created schema: {SCHEMA_NAME}")

# Helper function to generate random dates
def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)

# Helper to get a few future dates (for testing failure cases)
def future_date():
    return datetime.now() + timedelta(days=random.randint(1, 30))

# 1. Create users table WITHOUT constraints
conn.execute(f"""
CREATE TABLE {SCHEMA_NAME}.users (
    user_id VARCHAR,
    username VARCHAR,
    status VARCHAR,
    last_login TIMESTAMP
)
""")

# Insert data into users
users_data = []
statuses = ['active', 'inactive', 'pending', 'suspended']

# Generate mostly valid data with a few problematic rows
for i in range(1, 101):
    user_id = f"user_{i:03d}"
    username = f"username_{i:03d}"
    
    # For testing various violations:
    if i == 98:
        username = None  # NULL violation
    elif i == 99:
        username = "username_001"  # Duplicate violation
    elif i == 100:
        user_id = "user_" + "x" * 30  # Length violation
    
    status = statuses[i % 4]
    if i == 97:
        status = "deleted"  # Invalid status
        
    # Generate a past date for most records
    if i == 96:
        last_login = future_date()  # Future date violation
    else:
        last_login = random_date(datetime(2023, 1, 1), datetime.now())
    
    users_data.append((user_id, username, status, last_login))

conn.executemany(
    f"INSERT INTO {SCHEMA_NAME}.users VALUES (?, ?, ?, ?)",
    users_data
)
print(f"Inserted {len(users_data)} rows into users table")

# 2. Create orders table WITHOUT constraints
conn.execute(f"""
CREATE TABLE {SCHEMA_NAME}.orders (
    order_id VARCHAR,
    order_date TIMESTAMP,
    amount DECIMAL
)
""")

# Insert data into orders
orders_data = []
for i in range(1, 101):
    order_id = f"order_{i:03d}"
    
    # For testing violations:
    if i == 99:
        order_id = "order_001"  # Duplicate violation
    
    if i == 98:
        order_date = future_date()  # Future date violation
    else:
        order_date = random_date(datetime(2023, 1, 1), datetime.now())
    
    amount = round(random.uniform(10, 1000), 2)
    if i == 97:
        amount = -100.00  # Negative amount violation
    elif i == 96:
        amount = None  # NULL amount violation
    
    orders_data.append((order_id, order_date, amount))

conn.executemany(
    f"INSERT INTO {SCHEMA_NAME}.orders VALUES (?, ?, ?)",
    orders_data
)
print(f"Inserted {len(orders_data)} rows into orders table")

# 3. Create currencies table WITHOUT constraints
conn.execute(f"""
CREATE TABLE {SCHEMA_NAME}.currencies (
    exchange_from_ccy VARCHAR,
    exchange_to_ccy VARCHAR,
    reporting_date DATE,
    exchange_rate DECIMAL
)
""")

# Insert data into currencies
currencies_data = []
currency_pairs = [
    ('USD', 'EUR'), ('EUR', 'USD'),
    ('GBP', 'USD'), ('USD', 'GBP'),
    ('JPY', 'USD'), ('USD', 'JPY'),
    ('USD', 'CAD'), ('CAD', 'USD')
]

# Generate exchange rates for the last 30 days
for pair in currency_pairs:
    from_ccy, to_ccy = pair
    
    for i in range(30):
        date = datetime.now() - timedelta(days=i)
        
        # Default exchange rate
        exchange_rate = round(random.uniform(0.5, 2.0), 6)
        
        # For testing violations
        if from_ccy == 'USD' and to_ccy == 'EUR' and i == 0:
            date = future_date()  # Future date violation
        elif from_ccy == 'GBP' and to_ccy == 'USD' and i == 0:
            exchange_rate = -0.5  # Negative rate violation
        
        currencies_data.append((from_ccy, to_ccy, date, exchange_rate))

# Add NULL entries for testing
currencies_data.append((None, 'USD', datetime.now() - timedelta(days=5), 1.2))  # NULL from_ccy
currencies_data.append(('USD', None, datetime.now() - timedelta(days=5), 1.1))  # NULL to_ccy
currencies_data.append(('EUR', 'GBP', None, 0.9))  # NULL date
currencies_data.append(('CAD', 'EUR', datetime.now() - timedelta(days=3), None))  # NULL rate

conn.executemany(
    f"INSERT INTO {SCHEMA_NAME}.currencies VALUES (?, ?, ?, ?)",
    currencies_data
)
print(f"Inserted {len(currencies_data)} rows into currencies table")

# 4. Create cheese table WITHOUT constraints
conn.execute(f"""
CREATE TABLE {SCHEMA_NAME}.cheese (
    id VARCHAR,
    status VARCHAR
)
""")

# Insert data into cheese
cheese_data = []
cheese_statuses = ['old', 'fresh', 'pending']

for i in range(1, 51):
    cheese_id = f"cheese_{i:03d}"
    
    # For testing violations:
    if i == 49:
        cheese_id = "cheese_" + "x" * 30  # Length violation
    
    status = cheese_statuses[i % 3]
    if i == 50:
        status = "moldy"  # Invalid status
    elif i == 48:
        status = None  # NULL status
    
    cheese_data.append((cheese_id, status))

conn.executemany(
    f"INSERT INTO {SCHEMA_NAME}.cheese VALUES (?, ?)",
    cheese_data
)
print(f"Inserted {len(cheese_data)} rows into cheese table")

# 5. Create chess table WITHOUT constraints
conn.execute(f"""
CREATE TABLE {SCHEMA_NAME}.chess (
    game_id VARCHAR,
    date TIMESTAMP
)
""")

# Insert data into chess
chess_data = []
for i in range(1, 41):
    game_id = f"game_{i:03d}"
    
    # For testing violations:
    if i == 39:
        game_id = "game_001"  # Duplicate violation
    elif i == 38:
        game_id = None  # NULL game_id
    
    if i == 40:
        game_date = future_date()  # Future date violation
    elif i == 37:
        game_date = None  # NULL date
    else:
        game_date = random_date(datetime(2023, 1, 1), datetime.now())
    
    chess_data.append((game_id, game_date))

conn.executemany(
    f"INSERT INTO {SCHEMA_NAME}.chess VALUES (?, ?)",
    chess_data
)
print(f"Inserted {len(chess_data)} rows into chess table")

# Close the connection
conn.close()

print(f"\nDuckDB database created successfully: {DB_FILE}")
print(f"Created schema: {SCHEMA_NAME}")
print(f"Created tables: users, orders, currencies, cheese, chess")