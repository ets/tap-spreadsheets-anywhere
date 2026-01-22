# Schema Evolution with Target Integration

## The Challenge You Identified

You're correct - **schema evolution in the tap ≠ automatic schema evolution in the target**. Here's what happens:

### Without Target Schema Evolution Support:
```
Tap: Discovers new column 'phone' → Emits schema with 'phone' field
Target: Receives 'phone' data → ❌ FAILS because table doesn't have 'phone' column
```

### With Target Schema Evolution Support:
```
Tap: Discovers new column 'phone' → Emits schema with 'phone' field  
Target: Receives 'phone' data → ✅ Adds 'phone' column to table → Loads data
```

## Target-Snowflake Schema Evolution

The pipelinewise target-snowflake has **built-in schema evolution** support:

### Automatic Schema Changes:
- ✅ **Add new columns** when detected
- ✅ **Widen column types** (VARCHAR(50) → VARCHAR(100))
- ✅ **Handle type changes** (INT → VARCHAR for safety)
- ❌ **Does NOT drop columns** (for safety)

### How It Works:
1. Target receives schema from tap
2. Compares with existing table schema
3. Generates `ALTER TABLE` statements
4. Adds missing columns before loading data

## Complete Setup

### 1. Environment Variables (.env)
```bash
# Snowflake connection
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema

# Optional: Enable detailed logging
MELTANO_LOG_LEVEL=debug
```

### 2. Tap Configuration
```yaml
# In meltano.yml, configure tap settings
environments:
- name: dev
  config:
    plugins:
      extractors:
      - name: tap-spreadsheets-anywhere
        config:
          tables:
          - name: customers
            path: s3://your-bucket/customers/
            pattern: "customers_*.csv"
            start_date: "2023-01-01T00:00:00Z"
            key_properties: [customer_id]
            format: csv
            
            # Enable schema evolution in tap
            schema_evolution_mode: merge
            evolve_schema_during_sync: true
            evolution_sample_size: 200
            
          # Global evolution settings
          default_schema_evolution_mode: merge
          default_evolve_schema_during_sync: true
          schema_cache_ttl_hours: 24
```

### 3. Target Configuration  
```yaml
# Target-snowflake with schema evolution
- name: target-snowflake
  config:
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: ${SNOWFLAKE_WAREHOUSE}
    dbname: ${SNOWFLAKE_DATABASE}
    default_target_schema: ${SNOWFLAKE_SCHEMA}
    
    # Enable schema evolution in target
    add_metadata_columns: true
    flush_all_streams: false
    
    # File format for staging
    file_format: csv
    
    # Handle schema changes gracefully  
    data_flattening_max_level: 5
```

## Testing Schema Evolution End-to-End

### Test Scenario 1: New Column Addition

**Step 1:** Initial data
```csv
# customers_v1.csv
customer_id,name,email
1,Alice,alice@email.com
2,Bob,bob@email.com
```

**Step 2:** Run initial sync
```bash
# Create config
meltano config set tap-spreadsheets-anywhere tables '[
  {
    "name": "customers",
    "path": "file://./test_data",
    "pattern": "customers_v1.csv", 
    "start_date": "2020-01-01T00:00:00Z",
    "key_properties": ["customer_id"],
    "format": "csv",
    "schema_evolution_mode": "merge",
    "evolve_schema_during_sync": true
  }
]'

# Set target config
meltano config set target-snowflake account $SNOWFLAKE_ACCOUNT
meltano config set target-snowflake user $SNOWFLAKE_USER  
meltano config set target-snowflake password $SNOWFLAKE_PASSWORD
meltano config set target-snowflake warehouse $SNOWFLAKE_WAREHOUSE
meltano config set target-snowflake dbname $SNOWFLAKE_DATABASE
meltano config set target-snowflake default_target_schema $SNOWFLAKE_SCHEMA

# Initial sync
meltano run tap-spreadsheets-anywhere target-snowflake
```

**Result:** Table created with columns: `customer_id`, `name`, `email`

**Step 3:** Evolved data
```csv
# customers_v2.csv  
customer_id,name,email,phone,status
1,Alice,alice@email.com,555-0001,active
2,Bob,bob@email.com,555-0002,active
3,Charlie,charlie@email.com,555-0003,inactive
```

**Step 4:** Update config and sync
```bash
# Update pattern to include v2 file
meltano config set tap-spreadsheets-anywhere tables '[
  {
    "name": "customers",
    "path": "file://./test_data", 
    "pattern": "customers_v2.csv",
    "start_date": "2020-01-01T00:00:00Z",
    "key_properties": ["customer_id"],
    "format": "csv", 
    "schema_evolution_mode": "merge",
    "evolve_schema_during_sync": true
  }
]'

# Sync with evolved schema
meltano run tap-spreadsheets-anywhere target-snowflake
```

**Expected Result:** 
- Tap detects new columns `phone`, `status`
- Emits evolved schema  
- Target-snowflake receives evolved schema
- Adds columns: `ALTER TABLE customers ADD COLUMN phone VARCHAR`, `ADD COLUMN status VARCHAR`
- Loads data successfully

### Test Scenario 2: Type Widening

**Data Evolution:**
```csv
# Before: customer_id as integer
customer_id,name
1,Alice
2,Bob

# After: customer_id as UUID  
customer_id,name
550e8400-e29b-41d4-a716-446655440000,Alice
6ba7b810-9dad-11d1-80b4-00c04fd430c8,Bob
```

**Expected Result:**
- Tap detects type change: `integer` → `string`
- Target widens column: `ALTER TABLE customers ALTER COLUMN customer_id TYPE VARCHAR`

## Monitoring Schema Changes

### 1. Check Tap Logs
```bash
# Run with detailed logging
meltano --log-level=debug run tap-spreadsheets-anywhere target-snowflake

# Look for evolution events
grep -i "schema.*evolv" logs/meltano.log
grep -i "added.*field" logs/meltano.log
```

### 2. Check Target Logs  
```bash
# Look for ALTER TABLE statements
grep -i "alter table" logs/meltano.log
grep -i "add column" logs/meltano.log
```

### 3. Verify in Snowflake
```sql
-- Check table structure
DESCRIBE TABLE your_schema.customers;

-- Check for new columns
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'YOUR_SCHEMA'
  AND table_name = 'CUSTOMERS'
ORDER BY ordinal_position;

-- View recent data to confirm new fields
SELECT * FROM your_schema.customers 
WHERE phone IS NOT NULL  -- New column
LIMIT 10;
```

## Advanced Configuration

### Handling Different Evolution Scenarios

```yaml
# Production-safe configuration
tap-spreadsheets-anywhere:
  tables:
  # Critical tables - strict mode
  - name: financial_transactions
    schema_evolution_mode: strict
    evolve_schema_during_sync: false
    
  # Regular tables - merge mode  
  - name: customer_data
    schema_evolution_mode: merge
    evolve_schema_during_sync: true
    evolution_sample_size: 500
    
  # Experimental tables - replace mode
  - name: api_logs
    schema_evolution_mode: replace
    evolve_schema_during_sync: true
    prefer_schema_as_string: true

target-snowflake:
  # Conservative target settings
  add_metadata_columns: true
  flush_all_streams: false
  hard_delete: false  # Keeps schema evolution safe
```

## Troubleshooting Common Issues

### Issue 1: Target Doesn't Support Schema Evolution
**Problem:** Older target versions don't handle schema changes
**Solution:** 
```bash
# Upgrade target-snowflake
meltano upgrade target-snowflake

# Or use alternative targets with evolution support
meltano add loader target-postgres --variant=pipelinewise
```

### Issue 2: Type Conflicts
**Problem:** Incompatible type changes (string → integer)
**Solution:**
```yaml
# Use schema overrides to force types
schema_overrides:
  customer_id:
    type: ["null", "string"]  # Force to string
  amount: 
    type: ["null", "number"]  # Force to number
```

### Issue 3: Too Many Schema Changes
**Problem:** Frequent schema changes cause performance issues
**Solution:**
```yaml
# Reduce evolution frequency
evolve_schema_during_sync: false  # Only evolve during discovery
schema_cache_ttl_hours: 48        # Longer cache

# Or use staging schema
target-snowflake:
  default_target_schema: staging   # Separate schema for evolution
```

## Summary

✅ **Tap Schema Evolution:** Detects and adapts to data changes
✅ **Target Schema Evolution:** Automatically modifies target schema  
✅ **End-to-End Evolution:** Changes flow from source → tap → target → warehouse
⚠️ **Important:** Both tap AND target must support schema evolution for full automation

The pipelinewise target-snowflake supports schema evolution, so your evolved schemas from the tap will automatically create the necessary columns in Snowflake tables.