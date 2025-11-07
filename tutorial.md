# dbt with DuckDB Tutorial

## Initial Setup

```bash
# Explore the database
# Using DuckDB CLI (if installed):
duckdb wow.duckdb "SHOW TABLES"
duckdb wow.duckdb "DESCRIBE churn_features"

# Or using Python:
python -c "import duckdb; conn = duckdb.connect('wow.duckdb', read_only=True); print(conn.execute('SHOW TABLES').df())"

# Activate environment and check dbt
source .venv/bin/activate
dbt --version

# Initialize project (if starting fresh)
dbt init wow_analytics
```

## Configure Profile

Create `wow_analytics/profiles.yml` with two environments:

```yaml
wow_analytics:
  outputs:
    dev:
      type: duckdb
      path: '../wow_test.duckdb'
      schema: 'main'
      threads: 1
    prod:
      type: duckdb
      path: '../wow.duckdb'
      schema: 'main'
      threads: 1
  target: dev  # Default to dev to protect production
```

This setup:
- **dev** target uses `wow_test.duckdb` for safe experimentation
- **prod** target uses `wow.duckdb` for production data
- Default is `dev` to prevent accidental production changes

## Create Test Database

Copy production database to create a test environment:

```bash
cp wow.duckdb wow_test.duckdb
```

## Test Connection

```bash
cd wow_analytics
source ../.venv/bin/activate

# Test dev connection (default)
dbt debug

# Test prod connection (requires --target flag)
dbt debug --target prod
```

## Create Models

```bash
# Remove example models
rm -rf models/example

# Create directories
mkdir -p models/staging models/analytics
```

Create models:
- `models/sources.yml` - Define sources with `schema: main`
- `models/staging/stg_players.sql` - Staging view
- `models/analytics/player_segments.sql` - Player segmentation
- `models/analytics/churn_analysis.sql` - Churn metrics
- Add `schema.yml` files with tests

Update `dbt_project.yml` to set materializations:
```yaml
models:
  wow_analytics:
    staging:
      +materialized: view
    analytics:
      +materialized: table
```

## Run dbt

### Development (Default)

```bash
# Build models in dev (wow_test.duckdb)
dbt run

# Run tests in dev
dbt test
```

### Production (Explicit)

```bash
# Build models in production (wow.duckdb)
dbt run --target prod

# Run tests in production
dbt test --target prod
```

## Query Results

### Development Database

```bash
# Using DuckDB CLI (if installed):
duckdb wow_test.duckdb "SHOW TABLES"
duckdb wow_test.duckdb "SELECT * FROM main.churn_analysis ORDER BY churn_rate DESC LIMIT 10"

# Or using Python:
python -c "import duckdb; print(duckdb.connect('wow_test.duckdb').execute('SELECT * FROM main.churn_analysis ORDER BY churn_rate DESC LIMIT 10').df())"
```

### Production Database

```bash
# Using DuckDB CLI (if installed):
duckdb wow.duckdb "SHOW TABLES"
duckdb wow.duckdb "SELECT * FROM main.churn_analysis ORDER BY churn_rate DESC LIMIT 10"

# Or using Python (read-only recommended):
python -c "import duckdb; print(duckdb.connect('wow.duckdb', read_only=True).execute('SELECT * FROM main.churn_analysis ORDER BY churn_rate DESC LIMIT 10').df())"
```

## Common Commands

```bash
dbt run --select model_name   # Build specific model
dbt docs generate && dbt docs serve   # Documentation
dbt clean   # Clean build artifacts
dbt compile # Compile models
```
