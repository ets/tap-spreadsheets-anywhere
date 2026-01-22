# Quick Test Commands for Schema Evolution & Catalog Discovery

## Prerequisites
```bash
cd /home/prashant/tap-spreadsheets-anywhere
pip install -e .
```

## Test 1: Basic Discovery
```bash
# Create simple test config
cat > test_config.json << 'EOF'
{
  "tables": [
    {
      "name": "orders",
      "path": "file://./artifacts",
      "pattern": "orders.csv",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["order_number"],
      "format": "csv",
      "schema_evolution_mode": "merge",
      "evolve_schema_during_sync": true
    }
  ]
}
EOF

# Run discovery
meltano --no-environment invoke --config=test_config.json tap-spreadsheets-anywhere --discover
```

## Test 2: Schema Evolution Discovery
```bash
# Create config with evolution enabled
cat > evolution_config.json << 'EOF'
{
  "default_schema_evolution_mode": "merge",
  "default_evolve_schema_during_sync": true,
  "schema_cache_ttl_hours": 1,
  
  "tables": [
    {
      "name": "orders",
      "path": "file://./artifacts", 
      "pattern": "orders.csv",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["order_number"],
      "format": "csv",
      "evolution_sample_size": 100
    }
  ]
}
EOF

# Discover with evolution
meltano --no-environment invoke --config=evolution_config.json tap-spreadsheets-anywhere --discover
```

## Test 3: Sync with Evolution
```bash
# Run sync to test runtime evolution
meltano --no-environment invoke --config=evolution_config.json tap-spreadsheets-anywhere > sync_output.jsonl 2> sync_logs.txt

# Check logs for evolution events
grep -i "evolution\|schema" sync_logs.txt
```

## Test 4: Cache Testing
```bash
# First run - generates cache
time meltano --no-environment invoke --config=evolution_config.json tap-spreadsheets-anywhere --discover > /dev/null

# Check cache files
ls -la .tap_*.json

# Second run - uses cache (should be faster)
time meltano --no-environment invoke --config=evolution_config.json tap-spreadsheets-anywhere --discover > /dev/null
```

## Test 5: Force Cache Regeneration  
```bash
cat > force_regen_config.json << 'EOF'
{
  "force_catalog_regeneration": true,
  "tables": [
    {
      "name": "orders",
      "path": "file://./artifacts",
      "pattern": "orders.csv", 
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["order_number"],
      "format": "csv"
    }
  ]
}
EOF

meltano --no-environment invoke --config=force_regen_config.json tap-spreadsheets-anywhere --discover
```

## Test 6: Strict Mode (Should Fail on Changes)
```bash
cat > strict_config.json << 'EOF'
{
  "tables": [
    {
      "name": "orders",
      "path": "file://./artifacts",
      "pattern": "orders.csv",
      "start_date": "2020-01-01T00:00:00Z", 
      "key_properties": ["order_number"],
      "format": "csv",
      "schema_evolution_mode": "strict",
      "evolve_schema_during_sync": false
    }
  ]
}
EOF

# This should work fine initially
meltano --no-environment invoke --config=strict_config.json tap-spreadsheets-anywhere --discover

# If you modify data to have schema changes, sync should fail in strict mode
```

## Test 7: View Schema State
```bash
# Check schema evolution history
cat .tap_schema_state.json | jq '.'

# Check cached catalog
cat .tap_catalog_cache.json | jq '.streams[0].schema.properties | keys'

# View evolution events specifically
cat .tap_schema_state.json | jq '.schemas[].evolution_history[]?'
```

## Test 8: Different Evolution Modes
```bash
# Test replace mode (most flexible)
cat > replace_config.json << 'EOF'
{
  "tables": [
    {
      "name": "orders",
      "path": "file://./artifacts",
      "pattern": "orders.csv",
      "start_date": "2020-01-01T00:00:00Z",
      "key_properties": ["order_number"], 
      "format": "csv",
      "schema_evolution_mode": "replace",
      "evolve_schema_during_sync": true,
      "evolution_sample_size": 50
    }
  ]
}
EOF

meltano --no-environment invoke --config=replace_config.json tap-spreadsheets-anywhere --discover
```

## Test 9: Performance Comparison
```bash
# Time discovery without caching
rm -f .tap_*.json
time meltano --no-environment invoke --config=evolution_config.json tap-spreadsheets-anywhere --discover > /dev/null

# Time discovery with caching  
time meltano --no-environment invoke --config=evolution_config.json tap-spreadsheets-anywhere --discover > /dev/null

echo "Cache performance difference should be significant for large datasets"
```

## Cleanup
```bash
rm -f test_config.json evolution_config.json force_regen_config.json strict_config.json replace_config.json
rm -f .tap_*.json sync_output.jsonl sync_logs.txt
```

## Monitoring Commands
```bash
# Watch for schema evolution in real-time
tail -f sync_logs.txt | grep -i "schema\|evolution"

# Check cache age
ls -la .tap_*.json

# View detailed schema comparison
cat .tap_schema_state.json | jq '.schemas[] | {current_schema: .current_schema, evolution_history: .evolution_history}'
```