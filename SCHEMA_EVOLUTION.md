# Schema Evolution Guide for tap-spreadsheets-anywhere

## Overview

Schema evolution allows the tap to automatically adapt to changes in your data structure over time. This is essential for real-world data pipelines where source data schemas can change due to application updates, new features, or data source modifications.

## Features

### 1. **Automatic Schema Detection**
- Samples data files to detect schema changes
- Configurable sampling rate and sample size
- Statistical type inference with configurable preferences

### 2. **Multiple Evolution Modes**
- **`strict`**: Fail on any schema changes
- **`merge`**: Merge new fields and widen compatible types
- **`replace`**: Replace schema entirely with new detected schema  
- **`disabled`**: No schema evolution

### 3. **Type Safety**
- Safe type widening (integer → number → string)
- Backwards compatibility checks
- Configurable type preferences

### 4. **Runtime Evolution**
- Schema evolution during sync operations
- Per-file schema sampling
- Automatic schema re-emission when changes detected

## Configuration

### Table-Level Configuration

```json
{
  "tables": [{
    "name": "my_table",
    "path": "s3://bucket/path",
    "pattern": "*.csv",
    
    // Schema evolution settings
    "schema_evolution_mode": "merge",
    "evolve_schema_during_sync": true,
    "evolution_sample_size": 200,
    "schema_version": "1.0",
    
    // Existing schema override support
    "schema_overrides": {
      "field_name": {
        "type": ["null", "string"]
      }
    }
  }]
}
```

### Global Defaults

```json
{
  "default_schema_evolution_mode": "merge",
  "default_evolve_schema_during_sync": false,
  "default_evolution_sample_size": 100,
  "tables": [...]
}
```

## Evolution Modes Explained

### Strict Mode (`"strict"`)
```json
{
  "schema_evolution_mode": "strict",
  "evolve_schema_during_sync": false
}
```

**Use Case**: Critical data pipelines where schema stability is required
- ❌ Fails on any schema changes (new fields, type changes, removed fields)
- ✅ Ensures data consistency and prevents unexpected changes
- ⚠️ Requires manual intervention when source schema changes

### Merge Mode (`"merge"`) - **Recommended Default**
```json
{
  "schema_evolution_mode": "merge",
  "evolve_schema_during_sync": true,
  "evolution_sample_size": 100
}
```

**Use Case**: Most production scenarios where flexibility and safety are needed
- ✅ Adds new fields automatically
- ✅ Widens types safely (integer → number → string)
- ❌ Prevents unsafe type changes (string → integer)
- ⚠️ Warns about incompatible changes but continues processing

**Type Widening Examples**:
- `integer` → `number` (when decimals detected)
- `integer`/`number` → `string` (when non-numeric data detected)
- Always maintains `null` compatibility

### Replace Mode (`"replace"`)
```json
{
  "schema_evolution_mode": "replace",
  "evolve_schema_during_sync": true
}
```

**Use Case**: Highly dynamic data sources with frequent schema changes
- ✅ Adopts new schema completely
- ⚠️ May break downstream systems expecting previous schema
- 🔄 Useful for exploratory data analysis or rapidly changing APIs

### Disabled Mode (`"disabled"`)
```json
{
  "schema_evolution_mode": "disabled"
}
```

**Use Case**: When you want to control schema manually
- 📌 Uses only the initial discovered schema
- 💯 Relies entirely on `schema_overrides` for customization

## Best Practices

### 1. **Start Conservative, Become Flexible**
```json
// Initial setup - be strict to understand your data
{
  "schema_evolution_mode": "strict",
  "evolve_schema_during_sync": false
}

// After stabilization - enable safe evolution
{
  "schema_evolution_mode": "merge", 
  "evolve_schema_during_sync": true,
  "evolution_sample_size": 200
}
```

### 2. **Use Appropriate Sample Sizes**
```json
// For large files - smaller samples for performance
{
  "evolution_sample_size": 50
}

// For small files - larger samples for accuracy  
{
  "evolution_sample_size": 500
}

// For highly variable data - larger samples
{
  "evolution_sample_size": 1000
}
```

### 3. **Combine with Schema Overrides**
```json
{
  "schema_evolution_mode": "merge",
  "schema_overrides": {
    // Lock critical fields to specific types
    "customer_id": {"type": ["null", "string"]},
    "amount": {"type": ["null", "number"]},
    
    // Ensure date fields are properly typed
    "created_at": {
      "type": ["null", "string"],
      "format": "date-time"
    }
  }
}
```

### 4. **Environment-Specific Configuration**
```json
// Production - Conservative
{
  "default_schema_evolution_mode": "merge",
  "default_evolve_schema_during_sync": false
}

// Staging - Flexible 
{
  "default_schema_evolution_mode": "merge",
  "default_evolve_schema_during_sync": true
}

// Development - Very Flexible
{
  "default_schema_evolution_mode": "replace",  
  "default_evolve_schema_during_sync": true
}
```

## Examples

### Example 1: E-commerce Customer Data

**Initial Schema**:
```json
{
  "customer_id": {"type": ["null", "integer"]},
  "name": {"type": ["null", "string"]},
  "email": {"type": ["null", "string"]}
}
```

**New Data Introduces**:
- `phone` field (new)
- `customer_id` as UUID string (type change)
- `loyalty_points` as decimal (new)

**With Merge Mode**:
```json
{
  "customer_id": {"type": ["null", "string"]}, // Widened integer → string
  "name": {"type": ["null", "string"]},
  "email": {"type": ["null", "string"]},
  "phone": {"type": ["null", "string"]}, // Added
  "loyalty_points": {"type": ["null", "number"]} // Added
}
```

### Example 2: API Response Evolution

**Configuration**:
```json
{
  "name": "api_responses",
  "schema_evolution_mode": "merge",
  "evolve_schema_during_sync": true,
  "evolution_sample_size": 100,
  "schema_overrides": {
    "api_version": {"type": ["null", "string"]},
    "timestamp": {
      "type": ["null", "string"],
      "format": "date-time"
    }
  }
}
```

This ensures:
- New API fields are automatically detected
- Critical fields (`api_version`, `timestamp`) maintain consistent types
- Schema adapts to API changes without manual intervention

### Example 3: Log File Processing

**Configuration**:
```json
{
  "name": "application_logs",
  "schema_evolution_mode": "replace",
  "evolve_schema_during_sync": true,
  "evolution_sample_size": 50,
  "prefer_schema_as_string": true
}
```

This configuration:
- Completely replaces schema when log format changes
- Uses small samples for performance (logs are typically large)
- Treats all fields as strings for maximum flexibility

## Monitoring and Troubleshooting

### Log Messages to Watch For

**Schema Evolution Events**:
```
INFO - Schema evolved for stream customer_data
INFO - Added new field to schema: phone_number
INFO - Evolved field type: customer_id from ['null', 'integer'] to ['null', 'string']
```

**Warnings**:
```
WARNING - Cannot safely evolve field amount from ['null', 'string'] to ['null', 'number']
WARNING - Schema evolution sampling failed for file s3://bucket/corrupt_file.csv: JSON decode error
```

**Errors**:
```
ERROR - Schema evolution not allowed in strict mode. Changes: {'added_fields': ['new_field']}
ERROR - Schema evolution failed for stream orders: Invalid evolution configuration
```

### Common Issues and Solutions

**Issue**: Schema evolution fails with type conflicts
```
ERROR - Cannot safely evolve field 'price' from ['null', 'string'] to ['null', 'integer']
```
**Solution**: Use `schema_overrides` to force the desired type or switch to `replace` mode

**Issue**: Too many schema changes detected
```
WARNING - 50+ fields changed, consider reviewing data source
```
**Solution**: Check data quality, adjust sample size, or use `strict` mode temporarily

**Issue**: Performance degradation with schema evolution
```
WARNING - Schema evolution sampling taking >5s per file
```
**Solution**: Reduce `evolution_sample_size` or disable `evolve_schema_during_sync`

## Migration Guide

### From Manual Schema Management

**Before**:
```json
{
  "schema_overrides": {
    "field1": {"type": ["null", "string"]},
    "field2": {"type": ["null", "integer"]},
    "field3": {"type": ["null", "number"]}
  }
}
```

**After** (Gradual Migration):
```json
{
  "schema_evolution_mode": "merge",
  "evolve_schema_during_sync": false, // Start conservative
  "schema_overrides": {
    // Keep critical overrides, remove others gradually
    "field1": {"type": ["null", "string"]}
  }
}
```

### Enabling Runtime Evolution

**Phase 1** - Discovery:
```json
{
  "schema_evolution_mode": "merge",
  "evolve_schema_during_sync": false
}
```

**Phase 2** - Controlled Evolution:
```json
{
  "schema_evolution_mode": "merge",
  "evolve_schema_during_sync": true,
  "evolution_sample_size": 50  // Small sample
}
```

**Phase 3** - Full Evolution:
```json
{
  "schema_evolution_mode": "merge",
  "evolve_schema_during_sync": true,
  "evolution_sample_size": 200  // Full sample
}
```

This implementation provides comprehensive schema evolution capabilities while maintaining backward compatibility and safety for your data pipelines.