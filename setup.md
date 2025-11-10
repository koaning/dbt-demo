# WoW Analytics dbt Setup

## Project Structure

```
wow_analytics/
├── dbt_project.yml         # Project configuration
├── profiles.yml            # Local connection config (DuckDB)
├── .gitignore              # Git ignore patterns
├── .user.yml               # User-specific project ID
├── README.md               # Default dbt readme
│
├── models/                 # SQL models (transformations)
│   ├── sources.yml         # Defines source tables from wow.duckdb
│   └── staging/
│       ├── stg_wowlogs_with_sessions.sql  # Add session IDs (view)
│       ├── player_session_stats.sql       # Bot detection (table)
│       ├── stg_wowlogs_no_bots.sql        # Filtered logs (table)
│       ├── stg_players.sql                # Player aggregates (view)
│       └── schema.yml                     # Tests for staging models
│
├── analyses/               # Ad-hoc analytical queries (not materialized)
│   └── .gitkeep
│
├── macros/                 # Jinja macros for reusable SQL logic
│   └── .gitkeep
│
├── seeds/                  # CSV files loaded as tables
│   └── .gitkeep
│
├── snapshots/              # Type-2 slowly changing dimension tables
│   └── .gitkeep
│
├── tests/                  # Custom data tests
│   └── .gitkeep
│
├── logs/                   # dbt execution logs (gitignored)
│   └── dbt.log
│
└── target/                 # Compiled SQL and artifacts (gitignored)
    ├── manifest.json       # Full project manifest
    ├── catalog.json        # Documentation catalog
    ├── compiled/           # Compiled SQL files
    └── run/                # Executed SQL files
```

## Configuration

**Connection**: Defined in `profiles.yml`
- Uses DuckDB adapter (`dbt-duckdb`)
- Uses `main` schema
- Profile name: `wow_analytics`

**Environments**:
- **dev** (default): Points to `../wow_test.duckdb` - Safe for testing and development
- **prod**: Points to `../wow.duckdb` - Production data (protected by default)

**Models**: Configured in `dbt_project.yml`
- Staging models: materialized as views
- Analytics models: materialized as tables

### Why Two Databases?

This setup protects production data by:
- **Default target is `dev`** - All commands use the test database unless you explicitly specify `--target prod`
- **Test database is isolated** - Experiment freely without affecting production
- **Production requires explicit flag** - Must consciously choose to modify prod data

## Directory Guide

### Core Directories

- **models/** - SQL transformations that create tables/views in your database
  - Define your data models using SELECT statements
  - Use Jinja templating and dbt-specific syntax ({{ source() }}, {{ ref() }})
  - Organized by layer (staging, analytics, etc.)

- **seeds/** - CSV files that dbt loads as tables
  - Useful for small lookup tables, mappings, or test data
  - Run `dbt seed` to load CSVs into database
  - Keep files under 1MB for performance

- **snapshots/** - Type-2 slowly changing dimensions (SCD)
  - Track historical changes to mutable data
  - Uses `timestamp` or `check` strategy to detect changes
  - Run `dbt snapshot` to capture point-in-time data

- **tests/** - Custom data quality tests
  - Write SQL that returns failing rows
  - Extends built-in tests (unique, not_null, etc.)
  - Run `dbt test` to validate data

- **analyses/** - Ad-hoc queries for analysis
  - SQL files that are compiled but not executed
  - Use `dbt compile` to get rendered SQL
  - Good for sharing queries with analysts

- **macros/** - Reusable Jinja functions/macros
  - DRY up SQL with custom functions
  - Can generate dynamic SQL
  - Example: macros for date calculations, pivots, etc.

### Build Artifacts (Gitignored)

- **target/** - Compiled SQL and execution artifacts
  - `manifest.json` - Full project graph and metadata
  - `compiled/` - Rendered SQL before execution
  - `run/` - SQL that was actually executed

- **logs/** - Execution logs
  - `dbt.log` - Debug information from dbt runs

## Source Data

The project reads from `wow.duckdb` (or `wow_test.duckdb` for dev):
- **wowlogs**: Event-level player activity logs with columns:
  - `player_id`: Unique player identifier
  - `guild`: Guild ID (numeric)
  - `level`: Character level at time of event
  - `race`: Character race (Human, Orc, Tauren, etc.)
  - `class`: Character class (Warrior, Mage, Rogue, etc.)
  - `where`: Location where event occurred
  - `datetime`: Timestamp of the event

## Data Pipeline

The project uses a multi-stage pipeline to clean and enrich the raw event logs:

```
wowlogs (raw)
    ↓
stg_wowlogs_with_sessions (view) ← Add session IDs
    ↓
player_session_stats (table) ← Calculate session metrics & detect bots
    ↓
stg_wowlogs_no_bots (table) ← Filter out bots
    ↓
stg_players (view) ← Aggregate to player level
```

## Current Models

### Staging Layer

#### 1. stg_wowlogs_with_sessions (view)
Adds session IDs to raw event logs:
- **Session definition**: Events within 30 minutes are part of the same session
- **Output**: Original columns + `session_id`, `session_number`, `is_session_start`
- **Purpose**: Enable session-based analysis

#### 2. player_session_stats (table)
Session statistics per player for bot detection:
- **Metrics**: Max/avg/median session duration, events per session, total sessions
- **Bot detection heuristics**:
  - `likely_bot_long_session`: Max session > 12 hours (720 minutes)
  - `likely_bot_avg_session`: Avg session > 6 hours (360 minutes)
  - `is_likely_bot`: Combined flag (either heuristic triggers)
- **Output**: One row per player with bot flags and session metrics
- **Result**: Detected 4,119 bots (4.5% of players accounting for 60% of events)
- **Tuning**: Adjust thresholds in `player_session_stats.sql` if needed

#### 3. stg_wowlogs_no_bots (table)
Clean event logs with bots filtered out:
- **Filtering**: Removes all events from players flagged as bots
- **Volume**: 36.5M events → 14.6M events after filtering
- **Players**: 91,064 players → 86,945 players after filtering

#### 4. stg_players (view)
Player-level aggregated features (bot-filtered):
- Calculates level statistics (max, min, avg)
- Counts active days, locations visited, and sessions
- Determines churn status (no activity in 30+ days)
- Tracks most recent class, race, and guild
- **Use case**: Clean player data ready for analysis or further modeling

## Common Commands

### Development (Default - Uses wow_test.duckdb)

```bash
# Activate environment
source ../.venv/bin/activate

# Test connection to dev database
dbt debug

# Build all models in dev
dbt run

# Build specific model in dev
dbt run --select stg_players

# Build model and downstream dependencies
dbt run --select player_segments+

# Build model and all upstream dependencies
dbt run --select +churn_analysis

# Run tests in dev
dbt test

# Load seed data (CSV files) to dev
dbt seed

# Capture snapshots in dev
dbt snapshot

# Compile without executing (useful for analyses/)
dbt compile

# Clean build artifacts
dbt clean

# Generate and serve documentation
dbt docs generate
dbt docs serve

# Full refresh (rebuild incrementals from scratch)
dbt run --full-refresh
```

### Production (Explicit Flag Required - Uses wow.duckdb)

```bash
# CAUTION: These commands modify production data!

# Test connection to prod database
dbt debug --target prod

# Build all models in production
dbt run --target prod

# Run tests in production
dbt test --target prod

# Load seeds to production
dbt seed --target prod

# Build specific model in production
dbt run --select player_segments --target prod
```

## Querying Data

### Development Database (wow_test.duckdb)

```bash
# Using DuckDB CLI (if installed)
duckdb wow_test.duckdb "SELECT * FROM main.stg_players LIMIT 10"

# Or using Python
python -c "import duckdb; print(duckdb.connect('wow_test.duckdb').execute('SELECT * FROM main.stg_players LIMIT 10').df())"

# Read-only mode (prevents accidental writes)
python -c "import duckdb; print(duckdb.connect('wow_test.duckdb', read_only=True).execute('SELECT * FROM main.stg_players LIMIT 10').df())"
```

### Useful Queries

**Bot detection statistics:**
```sql
SELECT
    is_likely_bot,
    count(*) as num_players,
    round(avg(max_session_duration_minutes), 2) as avg_max_session
FROM main.player_session_stats
GROUP BY is_likely_bot;
```

**Top bots by session length:**
```sql
SELECT
    player_id,
    max_session_duration_minutes / 60.0 as max_session_hours,
    total_sessions,
    total_events
FROM main.player_session_stats
WHERE is_likely_bot = 1
ORDER BY max_session_duration_minutes DESC
LIMIT 10;
```

**Session analysis for a specific player:**
```sql
SELECT
    session_id,
    min(datetime) as session_start,
    max(datetime) as session_end,
    count(*) as events_in_session,
    datediff('minute', min(datetime), max(datetime)) as duration_minutes
FROM main.stg_wowlogs_with_sessions
WHERE player_id = 83
GROUP BY session_id
ORDER BY session_start;
```

**Data quality metrics:**
```sql
SELECT
    'Original events' as metric,
    count(*) as value
FROM wowlogs
UNION ALL
SELECT
    'Events after bot removal',
    count(*)
FROM main.stg_wowlogs_no_bots
UNION ALL
SELECT
    'Percentage of events from bots',
    round(100.0 * (1 - count(*)::float / (SELECT count(*) FROM wowlogs)), 2)
FROM main.stg_wowlogs_no_bots;
```

### Production Database (wow.duckdb)

```bash
# Using DuckDB CLI (if installed)
duckdb wow.duckdb "SELECT * FROM main.stg_players LIMIT 10"

# Or using Python (read-only recommended)
python -c "import duckdb; print(duckdb.connect('wow.duckdb', read_only=True).execute('SELECT * FROM main.stg_players LIMIT 10').df())"
```

### Comparing Environments

```python
import duckdb

# Connect to both databases
dev_conn = duckdb.connect('wow_test.duckdb', read_only=True)
prod_conn = duckdb.connect('wow.duckdb', read_only=True)

# Compare player counts
dev_count = dev_conn.execute('SELECT COUNT(*) FROM main.stg_players').fetchone()[0]
prod_count = prod_conn.execute('SELECT COUNT(*) FROM main.stg_players').fetchone()[0]

print(f"Dev: {dev_count} players, Prod: {prod_count} players")

# Compare bot detection
dev_bots = dev_conn.execute('SELECT COUNT(*) FROM main.player_session_stats WHERE is_likely_bot = 1').fetchone()[0]
prod_bots = prod_conn.execute('SELECT COUNT(*) FROM main.player_session_stats WHERE is_likely_bot = 1').fetchone()[0]

print(f"Bots detected - Dev: {dev_bots}, Prod: {prod_bots}")
```

## DuckDB-Specific Notes

### File Locking
DuckDB uses file-based locking. If you get "Conflicting lock" errors:
- Close any open database connections (Python scripts, CLI sessions, notebooks)
- Only one process can write to the database at a time
- Use `read_only=True` for read-only connections: `duckdb.connect('wow.duckdb', read_only=True)`

### Performance Tips
- DuckDB excels at analytical queries on columnar data
- Use `COPY` or `read_parquet()` for large data imports
- Consider partitioning large tables by date/category
- DuckDB automatically parallelizes queries across CPU cores

### Extensions
Install DuckDB extensions for additional functionality:
```python
import duckdb
conn = duckdb.connect('wow.duckdb')
conn.execute("INSTALL httpfs")  # Read from S3/HTTP
conn.execute("INSTALL parquet")  # Enhanced Parquet support
conn.execute("LOAD httpfs")
```

## Database Management

### Refreshing Test Data from Production

Periodically sync test database with production data:

```bash
# Backup current test database (optional)
cp wow_test.duckdb wow_test.duckdb.backup

# Copy production to test
cp wow.duckdb wow_test.duckdb

# Rebuild dbt models in test
cd wow_analytics
dbt run
```

### Database Files

- **wow.duckdb** - Production database (289MB)
  - Should be backed up regularly
  - Consider adding to `.gitignore` if it contains sensitive data
  - Read with caution

- **wow_test.duckdb** - Development/test database (289MB)
  - Safe to delete and recreate
  - Add to `.gitignore` (not tracked in version control)
  - Experiment freely

### Git Best Practices

Update `.gitignore` to exclude databases:

```
# dbt
target/
dbt_packages/
logs/

# DuckDB databases
*.duckdb
*.duckdb.wal

# But track the production database if needed
!wow.duckdb
```

## Using demo.py with Marimo

The `demo.py` file uses the `DBT_ENV` environment variable to switch between databases:

```bash
# Run with dev database (default)
marimo run demo.py

# Run with production database
DBT_ENV=prod marimo run demo.py
```

The script automatically displays which database you're connected to:
- ✅ DEV database (wow_test.duckdb)
- 🚨 PRODUCTION database (wow.duckdb)

## Project Files

- **.gitignore** - Excludes `target/`, `logs/`, `dbt_packages/`, and test databases from version control
- **.user.yml** - Contains unique project ID (auto-generated by dbt)
- **profiles.yml** - Database connection config (can also use `~/.dbt/profiles.yml`)
- **demo.py** - Marimo notebook for querying dbt models (respects `DBT_ENV` variable)
