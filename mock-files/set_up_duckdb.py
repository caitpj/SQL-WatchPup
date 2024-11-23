import duckdb
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def create_test_database():
    """Create test database with sample data"""
    
    # Create connection to DuckDB (will create file if it doesn't exist)
    conn = duckdb.connect('data_quality_test.duckdb')
    
    try:
        # Create schemas
        conn.execute("CREATE SCHEMA IF NOT EXISTS sandbox")
        
        # Drop existing tables if they exist
        conn.execute("DROP TABLE IF EXISTS sandbox.users")
        conn.execute("DROP TABLE IF EXISTS sandbox.orders")
        
        # Create users table
        conn.execute("""
            CREATE TABLE sandbox.users (
                user_id INTEGER,
                username VARCHAR,
                email VARCHAR,
                status VARCHAR,
                created_date DATE,
                last_login TIMESTAMP
            )
        """)
        
        # Generate sample data for users
        current_date = datetime.now()
        users_data = pd.DataFrame({
            'user_id': range(1, 101),
            'username': [f'user_{i}' for i in range(1, 101)],
            'email': [f'user_{i}@example.com' for i in range(1, 101)],
            'status': np.random.choice(
                ['active', 'inactive', 'pending', 'suspended'],
                100
            ),
            'created_date': [
                current_date - timedelta(days=np.random.randint(1, 365))
                for _ in range(100)
            ],
            'last_login': [
                current_date - timedelta(days=np.random.randint(0, 30))
                for _ in range(100)
            ]
        })
        
        # Insert some test cases
        # Add NULL values
        users_data.loc[0:4, 'status'] = None
        
        # Add long usernames
        users_data.loc[5:7, 'username'] = 'this_is_a_very_long_username_that_exceeds_twenty_characters'
        
        # Add duplicate usernames
        users_data.loc[8, 'username'] = users_data.loc[9, 'username']
        
        # Add future dates
        users_data.loc[10:12, 'last_login'] = current_date + timedelta(days=10)
        
        # Insert data into users table
        conn.execute("INSERT INTO sandbox.users SELECT * FROM users_data")
        
        # Create orders table
        conn.execute("""
            CREATE TABLE sandbox.orders (
                order_id INTEGER,
                user_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2),
                status VARCHAR
            )
        """)
        
        # Generate sample data for orders
        orders_data = pd.DataFrame({
            'order_id': range(1, 201),
            'user_id': np.random.choice(range(1, 101), 200),
            'order_date': [
                current_date - timedelta(days=np.random.randint(0, 365))
                for _ in range(200)
            ],
            'amount': np.random.uniform(10, 1000, 200).round(2),
            'status': np.random.choice(['pending', 'completed', 'cancelled'], 200)
        })
        
        # Insert some test cases
        # Add NULL dates
        orders_data.loc[0:4, 'order_date'] = None
        
        # Add future dates
        orders_data.loc[5:7, 'order_date'] = current_date + timedelta(days=10)
        
        # Insert data into orders table
        conn.execute("INSERT INTO sandbox.orders SELECT * FROM orders_data")
        
        # Print verification info
        print("Database created successfully!")
        
        print("\nUsers table schema:")
        schema_users = conn.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'sandbox' AND table_name = 'users'
            ORDER BY ordinal_position
        """).fetchdf()
        print(schema_users.to_string())
        
        print("\nUsers table count:")
        count_users = conn.execute("SELECT COUNT(*) as count FROM sandbox.users").fetchone()[0]
        print(f"Total rows: {count_users}")
        
        print("\nOrders table schema:")
        schema_orders = conn.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'sandbox' AND table_name = 'orders'
            ORDER BY ordinal_position
        """).fetchdf()
        print(schema_orders.to_string())
        
        print("\nOrders table count:")
        count_orders = conn.execute("SELECT COUNT(*) as count FROM sandbox.orders").fetchone()[0]
        print(f"Total rows: {count_orders}")
        
        # Print some sample data
        print("\nSample data from users table:")
        sample_users = conn.execute("SELECT * FROM sandbox.users LIMIT 5").fetchdf()
        print(sample_users.to_string())
        
        print("\nSample data from orders table:")
        sample_orders = conn.execute("SELECT * FROM sandbox.orders LIMIT 5").fetchdf()
        print(sample_orders.to_string())
        
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_test_database()