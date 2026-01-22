#!/bin/bash

# Test Script for Schema Evolution and Catalog Discovery
# Usage: ./test_schema_evolution.sh

set -e

echo "🧪 Testing Schema Evolution and Catalog Discovery"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test data directory
TEST_DIR="test_data"
mkdir -p $TEST_DIR

echo -e "${YELLOW}📁 Setting up test data...${NC}"

# Create initial CSV with basic schema
cat > $TEST_DIR/customers_v1.csv << 'EOF'
customer_id,name,email
1,Alice Johnson,alice@email.com
2,Bob Smith,bob@email.com
3,Charlie Brown,charlie@email.com
EOF

# Create evolved CSV with new columns
cat > $TEST_DIR/customers_v2.csv << 'EOF'
customer_id,name,email,phone,status,created_at
1,Alice Johnson,alice@email.com,555-0001,active,2024-01-01
2,Bob Smith,bob@email.com,555-0002,active,2024-01-02
3,Charlie Brown,charlie@email.com,555-0003,inactive,2024-01-03
4,Diana Prince,diana@email.com,555-0004,active,2024-01-04
EOF

# Create JSON test data with type changes
cat > $TEST_DIR/transactions_v1.json << 'EOF'
[
  {"transaction_id": 1, "amount": 100, "currency": "USD"},
  {"transaction_id": 2, "amount": 250, "currency": "USD"},
  {"transaction_id": 3, "amount": 75, "currency": "EUR"}
]
EOF

cat > $TEST_DIR/transactions_v2.json << 'EOF'
[
  {"transaction_id": "txn_001", "amount": 100.50, "currency": "USD", "merchant": "Store A"},
  {"transaction_id": "txn_002", "amount": 250.75, "currency": "USD", "merchant": "Store B"},
  {"transaction_id": "txn_003", "amount": 75.25, "currency": "EUR", "merchant": "Store C"}
]
EOF

echo -e "${GREEN}✅ Test data created${NC}"

# Configuration for initial schema
cat > config_basic.json << 'EOF'
{
  "tables": [
    {
      "name": "customers",
      "path": "file://./test_data",
      "pattern": "customers_v1.csv",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["customer_id"],
      "format": "csv",
      
      "schema_evolution_mode": "merge",
      "evolve_schema_during_sync": true,
      "evolution_sample_size": 10
    }
  ]
}
EOF

# Configuration with schema evolution enabled
cat > config_evolution.json << 'EOF'
{
  "default_schema_evolution_mode": "merge",
  "default_evolve_schema_during_sync": true,
  "default_evolution_sample_size": 50,
  "disable_catalog_cache": false,
  "schema_cache_ttl_hours": 24,
  
  "tables": [
    {
      "name": "customers",
      "path": "file://./test_data",
      "pattern": "customers_*.csv",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["customer_id"],
      "format": "csv",
      
      "schema_evolution_mode": "merge",
      "evolve_schema_during_sync": true,
      "evolution_sample_size": 10
    },
    {
      "name": "transactions",
      "path": "file://./test_data",
      "pattern": "transactions_*.json",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["transaction_id"],
      "format": "json",
      
      "schema_evolution_mode": "merge",
      "evolve_schema_during_sync": true,
      "evolution_sample_size": 5
    }
  ]
}
EOF

# Configuration for strict mode (should fail on changes)
cat > config_strict.json << 'EOF'
{
  "tables": [
    {
      "name": "transactions",
      "path": "file://./test_data",
      "pattern": "transactions_*.json",
      "start_date": "2020-01-01T00:00:00Z", 
      "key_properties": ["transaction_id"],
      "format": "json",
      
      "schema_evolution_mode": "strict",
      "evolve_schema_during_sync": false
    }
  ]
}
EOF

echo -e "${YELLOW}📋 Test configurations created${NC}"

run_test() {
    local test_name="$1"
    local config_file="$2"
    local expected_result="$3"
    
    echo -e "\n${YELLOW}🧪 Running Test: $test_name${NC}"
    echo "Config: $config_file"
    echo "Expected: $expected_result"
    echo "----------------------------------------"
    
    if [[ $expected_result == "SHOULD_FAIL" ]]; then
        set +e  # Allow command to fail
        meltano --no-environment invoke --config="$config_file" tap-spreadsheets-anywhere --discover > /tmp/test_output.json 2>&1
        exit_code=$?
        set -e
        
        if [[ $exit_code -ne 0 ]]; then
            echo -e "${GREEN}✅ Test passed: Command failed as expected${NC}"
        else
            echo -e "${RED}❌ Test failed: Command should have failed but succeeded${NC}"
        fi
    else
        meltano --no-environment invoke --config="$config_file" tap-spreadsheets-anywhere --discover > /tmp/test_output.json
        echo -e "${GREEN}✅ Discovery completed successfully${NC}"
        
        # Show discovered schema
        echo "Discovered streams:"
        cat /tmp/test_output.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
for stream in data.get('streams', []):
    print(f\"  - {stream['stream']}: {len(stream['schema']['properties'])} fields\")
    for field, schema in stream['schema']['properties'].items():
        if not field.startswith('_smart'):
            print(f\"    {field}: {schema.get('type', 'unknown')}\")"
    fi
}

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 1: Basic Discovery${NC}"
run_test "Initial Discovery" "config_basic.json" "SHOULD_PASS"

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 2: Schema Evolution Discovery${NC}"
run_test "Evolution Discovery" "config_evolution.json" "SHOULD_PASS"

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 3: Strict Mode (Should Fail)${NC}"
# First discover with v1 data
cp test_data/transactions_v1.json test_data/transactions_current.json
meltano --no-environment invoke --config="config_strict.json" tap-spreadsheets-anywhere --discover > catalog_strict.json
echo "Initial discovery with v1 data completed"

# Now try with v2 data (should fail in strict mode during sync)
cp test_data/transactions_v2.json test_data/transactions_current.json
run_test "Strict Mode Evolution" "config_strict.json" "SHOULD_FAIL"

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 4: Full Sync with Evolution${NC}"
echo "Testing complete sync process with schema evolution..."

# Test sync process
meltano --no-environment invoke \
  --config="config_evolution.json" \
  tap-spreadsheets-anywhere \
  --discover > catalog_evolved.json

echo "Generated catalog with evolved schema"

# Show evolved catalog
echo "Evolved catalog streams:"
cat catalog_evolved.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
for stream in data.get('streams', []):
    print(f\"Stream: {stream['stream']}\")
    props = stream['schema']['properties']
    non_meta_fields = [k for k in props.keys() if not k.startswith('_smart')]
    print(f\"  Fields ({len(non_meta_fields)}): {', '.join(non_meta_fields)}\")"

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 5: Cache Behavior${NC}"

echo "Testing catalog caching..."

# First run - should generate cache
echo "Run 1: Generating cache"
time meltano --no-environment invoke --config="config_evolution.json" tap-spreadsheets-anywhere --discover > /dev/null

# Check if cache files were created
echo "Cache files created:"
ls -la .tap_*.json 2>/dev/null || echo "No cache files found"

# Second run - should use cache
echo "Run 2: Using cache (should be faster)"
time meltano --no-environment invoke --config="config_evolution.json" tap-spreadsheets-anywhere --discover > /dev/null

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 6: Force Cache Regeneration${NC}"

# Test force regeneration
cat > config_force_regen.json << 'EOF'
{
  "force_catalog_regeneration": true,
  "tables": [
    {
      "name": "customers",
      "path": "file://./test_data",
      "pattern": "customers_*.csv",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["customer_id"],
      "format": "csv",
      "schema_evolution_mode": "merge"
    }
  ]
}
EOF

echo "Testing forced cache regeneration"
meltano --no-environment invoke --config="config_force_regen.json" tap-spreadsheets-anywhere --discover > /dev/null
echo -e "${GREEN}✅ Forced regeneration completed${NC}"

echo -e "\n${YELLOW}🎯 TESTING SCENARIO 7: Schema Evolution History${NC}"

# Check evolution state
echo "Schema evolution state:"
if [[ -f .tap_schema_state.json ]]; then
    cat .tap_schema_state.json | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    schemas = data.get('schemas', {})
    print(f'Tracked streams: {len(schemas)}')
    for stream_name, stream_data in schemas.items():
        history = stream_data.get('evolution_history', [])
        print(f'  {stream_name}: {len(history)} evolution events')
        for event in history[-3:]:  # Show last 3 events
            print(f\"    {event.get('timestamp', 'unknown')}: {event.get('evolution_type', 'unknown')}\")
except:
    print('No valid state found')
"
else
    echo "No schema state file found"
fi

echo -e "\n${GREEN}🎉 ALL TESTS COMPLETED${NC}"
echo "========================================"
echo "Files generated:"
echo "  - Test data: test_data/"
echo "  - Configs: config_*.json"
echo "  - Catalogs: catalog_*.json"
echo "  - Cache: .tap_*.json"
echo ""
echo "🧹 Cleanup command:"
echo "rm -rf test_data config_*.json catalog_*.json .tap_*.json /tmp/test_output.json"