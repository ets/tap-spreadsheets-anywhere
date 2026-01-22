# Test Snowflake Target Integration
cd /home/prashant/tap-spreadsheets-anywhere

# Initialize Meltano project
meltano install

# Create test data with schema evolution
mkdir -p test_data

# Initial data
cat > test_data/customers_v1.csv << 'EOF'
customer_id,name,email  
1,Alice Johnson,alice@email.com
2,Bob Smith,bob@email.com
3,Charlie Brown,charlie@email.com
EOF

# Evolved data (new columns)
cat > test_data/customers_v2.csv << 'EOF'
customer_id,name,email,phone,status,created_at
1,Alice Johnson,alice@email.com,555-0001,active,2024-01-01
2,Bob Smith,bob@email.com,555-0002,active,2024-01-02  
3,Charlie Brown,charlie@email.com,555-0003,inactive,2024-01-03
4,Diana Prince,diana@email.com,555-0004,active,2024-01-04
EOF

# Configure tap for schema evolution
meltano config set tap-spreadsheets-anywhere tables '[
  {
    "name": "customers",
    "path": "file://./test_data",
    "pattern": "customers_*.csv",
    "start_date": "2020-01-01T00:00:00Z", 
    "key_properties": ["customer_id"],
    "format": "csv",
    "schema_evolution_mode": "merge",
    "evolve_schema_during_sync": true,
    "evolution_sample_size": 100
  }
]'

# Set global evolution settings
meltano config set tap-spreadsheets-anywhere default_schema_evolution_mode merge
meltano config set tap-spreadsheets-anywhere default_evolve_schema_during_sync true
meltano config set tap-spreadsheets-anywhere schema_cache_ttl_hours 1

# Configure Snowflake target (replace with your credentials)
meltano config set target-snowflake account YOUR_SNOWFLAKE_ACCOUNT
meltano config set target-snowflake user YOUR_USERNAME
meltano config set target-snowflake password YOUR_PASSWORD
meltano config set target-snowflake warehouse YOUR_WAREHOUSE
meltano config set target-snowflake dbname YOUR_DATABASE
meltano config set target-snowflake default_target_schema test_schema

# Enable target schema evolution 
meltano config set target-snowflake add_metadata_columns true
meltano config set target-snowflake flush_all_streams false

echo "🧪 Test 1: Initial Sync (3 columns)"
# Test initial sync with v1 data
cp test_data/customers_v1.csv test_data/customers_current.csv

meltano run tap-spreadsheets-anywhere target-snowflake

echo "✅ Check Snowflake: Table should have customer_id, name, email columns"

echo "🧪 Test 2: Schema Evolution Sync (5 columns)"  
# Test evolved sync with v2 data
cp test_data/customers_v2.csv test_data/customers_current.csv

# Force catalog regeneration to pick up changes
meltano config set tap-spreadsheets-anywhere force_catalog_regeneration true

meltano run tap-spreadsheets-anywhere target-snowflake

echo "✅ Check Snowflake: Table should now have phone, status, created_at columns added"

echo "🧪 Test 3: Verify in Snowflake"
echo "Run these SQL commands in Snowflake:"
echo ""
echo "-- Check table structure"
echo "DESCRIBE TABLE test_schema.customers;"
echo ""
echo "-- Check data with new columns"  
echo "SELECT * FROM test_schema.customers WHERE phone IS NOT NULL;"
echo ""
echo "-- Verify column additions"
echo "SELECT column_name, data_type FROM information_schema.columns"
echo "WHERE table_schema = 'TEST_SCHEMA' AND table_name = 'CUSTOMERS'"
echo "ORDER BY ordinal_position;"

# Cleanup
rm -f test_data/customers_current.csv