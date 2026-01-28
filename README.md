# Data Warehouse MCP Server

A Model Context Protocol (MCP) server that provides read-only access to multiple data warehouse platforms with built-in safety features and SQL dialect support.

## Overview

This MCP server acts as a data warehouse engineering assistant with access to multiple data platforms:

- **MaxCompute**: Alibaba Cloud offline data warehouse for batch processing
- **Hologres**: Real-time analytics and OLAP queries
- **MySQL/PolarDB**: Source systems and transactional databases
- **Redshift**: AWS data warehouse for EU data and regional analytics

## Features

- ✅ **Multi-platform support** - Connect to MaxCompute, Hologres, MySQL, PolarDB, and Redshift
- ✅ **Safety first** - Automatic detection and blocking of destructive operations (DELETE, DROP, TRUNCATE, etc.)
- ✅ **Smart LIMIT** - Automatically adds LIMIT clauses to SELECT queries to prevent large result sets
- ✅ **Dialect-aware** - Provides platform-specific SQL examples and best practices
- ✅ **Schema exploration** - Browse database schemas, tables, and columns
- ✅ **Query validation** - Validate SQL queries before execution
- ✅ **Read-only by default** - Requires explicit confirmation for any destructive operations

## Installation

### Prerequisites

- Python 3.10 or higher
- pip

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Configuration

Configure database connections using environment variables. Two formats are supported:

#### Legacy Format (Single Connection String)

```bash
# MaxCompute
export MAXCOMPUTE_CONNECTION="maxcompute://access_id:access_key@endpoint/project"

# Hologres (PostgreSQL-compatible)
export HOLOGRES_CONNECTION="postgresql://user:password@host:port/database"

# MySQL
export MYSQL_CONNECTION="mysql+pymysql://user:password@host:port/database"

# PolarDB (MySQL-compatible)
export POLARDB_CONNECTION="mysql+pymysql://user:password@host:port/database"

# Redshift
export REDSHIFT_CONNECTION="redshift+redshift_connector://user:password@host:port/database"
```

#### Multi-Instance Format (Recommended)

This format allows configuring multiple instances of the same platform type using individual parameters.

**Pattern**: `{TYPE}_{REGION}_{PROJECT}_{PARAM}`

**MaxCompute/DataWorks Examples**:
```bash
# MAXCOMPUTE:example_project_region1
export MAXCOMPUTE_REGION1_PROJECT1_TYPE="MAXCOMPUTE"
export MAXCOMPUTE_REGION1_PROJECT1_PROJECT="example_project_name"
export MAXCOMPUTE_REGION1_PROJECT1_ACCESSID="<your_access_id>"
export MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY="<your_access_key>"
export MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT="http://service.<region>.maxcompute.aliyun.com/api"

# DATAWORKS:example_project_region2 (DataWorks projects map to MaxCompute)
export DATAWORKS_REGION2_PROJECT2_TYPE="DATAWORKS"
export DATAWORKS_REGION2_PROJECT2_PROJECT="example_project_name_2"
export DATAWORKS_REGION2_PROJECT2_ACCESSID="<your_access_id>"
export DATAWORKS_REGION2_PROJECT2_ACCESSKEY="<your_access_key>"
export DATAWORKS_REGION2_PROJECT2_ENDPOINT="http://service.<region2>.maxcompute.aliyun.com/api"
```

**Hologres Examples**:
```bash
# HOLOGRES:example_db
export HOLO_REGION1_DB1_TYPE="HOLOGRES"
export HOLO_REGION1_DB1_HOST="<your-instance>.hologres.aliyuncs.com"
export HOLO_REGION1_DB1_USER="<your_user>"
export HOLO_REGION1_DB1_PASSWORD="<your_password>"
export HOLO_REGION1_DB1_DBNAME="<your_database>"
export HOLO_REGION1_DB1_PORT="80"
```

**MySQL/PolarDB Examples**:
```bash
# POLARDB:example_db
export POLARDB_REGION1_DB1_TYPE="POLARDB"
export POLARDB_REGION1_DB1_HOST="<your-instance>.rwlb.rds.aliyuncs.com"
export POLARDB_REGION1_DB1_USER="<your_user>"
export POLARDB_REGION1_DB1_PASSWORD="<your_password>"
export POLARDB_REGION1_DB1_DB="<your_database>"

# MYSQL:example_db
export MYSQL_REGION1_DB1_TYPE="MySQL"
export MYSQL_REGION1_DB1_HOST="<your-instance>.rds.aliyuncs.com"
export MYSQL_REGION1_DB1_USER="<your_user>"
export MYSQL_REGION1_DB1_PASSWORD="<your_password>"
export MYSQL_REGION1_DB1_DB="<your_database>"
```

**Redshift Examples**:
```bash
# REDSHIFT:example_cluster
export REDSHIFT_REGION1_CLUSTER1_TYPE="REDSHIFT"
export REDSHIFT_REGION1_CLUSTER1_HOST="<your-workgroup>.<region>.redshift-serverless.amazonaws.com"
export REDSHIFT_REGION1_CLUSTER1_PORT="5439"
export REDSHIFT_REGION1_CLUSTER1_DB="<your_database>"
export REDSHIFT_REGION1_CLUSTER1_USER="<your_user>"
export REDSHIFT_REGION1_CLUSTER1_PASSWORD="<your_password>"
```

**Benefits of Multi-Instance Format**:
- Support multiple instances of the same platform type
- Clearer separation of configuration parameters
- Each instance gets a unique identifier (e.g., `maxcompute_region1_project1`, `holo_region1_db1`)
- Better organization for complex multi-region/multi-project setups

**Note**: Both formats can be used simultaneously. Not all platforms need to be configured. The server will only enable tools for platforms with valid connection strings.

## Usage

### Running the Server

```bash
python -m src.dw_mcp.server
```

Or use it with an MCP client that supports stdio communication.

### Available Tools

#### 1. `list_platforms`
List all configured data warehouse platforms.

```json
{
  "name": "list_platforms"
}
```

#### 2. `get_platform_info`
Get detailed information about a specific platform (features, dialect, use cases).

```json
{
  "name": "get_platform_info",
  "arguments": {
    "platform": "maxcompute"
  }
}
```

#### 3. `execute_query`
Execute a SQL query on the specified platform. Automatically adds LIMIT for SELECT queries.

```json
{
  "name": "execute_query",
  "arguments": {
    "platform": "hologres",
    "query": "SELECT * FROM users WHERE created_at > '2024-01-01'",
    "limit": 100,
    "allow_destructive": false
  }
}
```

**Safety Features**:
- Automatically adds `LIMIT` to SELECT queries if not present
- Blocks destructive operations (DELETE, UPDATE, DROP, etc.) unless `allow_destructive` is true
- Validates SQL syntax before execution

#### 4. `validate_query`
Validate a SQL query for safety without executing it.

```json
{
  "name": "validate_query",
  "arguments": {
    "query": "SELECT * FROM table",
    "allow_destructive": false
  }
}
```

#### 5. `get_schema_info`
Get schema information (tables and columns) for a platform.

```json
{
  "name": "get_schema_info",
  "arguments": {
    "platform": "mysql",
    "schema": "public"
  }
}
```

#### 6. `get_example_queries`
Get platform-specific example queries.

```json
{
  "name": "get_example_queries",
  "arguments": {
    "platform": "redshift"
  }
}
```

### Available Prompts

#### 1. `explain-schema`
Get an explanation of a table's schema and structure.

```json
{
  "name": "explain-schema",
  "arguments": {
    "platform": "maxcompute",
    "table": "user_events"
  }
}
```

#### 2. `data-lineage`
Explain data lineage and dependencies for a table.

```json
{
  "name": "data-lineage",
  "arguments": {
    "table": "fact_sales"
  }
}
```

#### 3. `query-optimization`
Get suggestions for optimizing a query.

```json
{
  "name": "query-optimization",
  "arguments": {
    "platform": "hologres",
    "query": "SELECT * FROM large_table WHERE date > '2024-01-01'"
  }
}
```

## Platform-Specific Guidance

### MaxCompute
- **Use Case**: Offline data warehouse, batch processing
- **Best Practices**:
  - Use partitions for large tables (e.g., `WHERE ds='20240101'`)
  - Leverage distributed processing for aggregations
  - Use UDFs for complex transformations

### Hologres
- **Use Case**: Real-time analytics, OLAP queries
- **Best Practices**:
  - PostgreSQL-compatible syntax
  - Use column storage for analytics workloads
  - Leverage window functions for complex analytics

### MySQL/PolarDB
- **Use Case**: Source systems, transactional data
- **Best Practices**:
  - Use indexes for query performance
  - Prefer specific columns over SELECT *
  - Use LIMIT for exploratory queries

### Redshift
- **Use Case**: EU data, regional data warehouse
- **Best Practices**:
  - Use distribution keys for joins
  - Leverage sort keys for range queries
  - Use COPY for bulk data loading

## Safety and Security

### Read-Only Operations
By default, the server only allows read operations (SELECT, SHOW, DESCRIBE, etc.). Destructive operations are blocked unless explicitly confirmed.

### Destructive Operations
The following operations are considered destructive and require `allow_destructive: true`:
- `DROP TABLE/DATABASE/SCHEMA`
- `TRUNCATE`
- `DELETE FROM`
- `UPDATE ... SET`
- `INSERT INTO`
- `CREATE TABLE/DATABASE/SCHEMA`
- `ALTER TABLE`
- `MERGE INTO`

### Automatic LIMIT
SELECT queries without a LIMIT clause automatically get `LIMIT 100` added to prevent accidentally retrieving large result sets.

## Development

### Project Structure

```
dw-mcp/
├── src/
│   └── dw_mcp/
│       ├── __init__.py
│       ├── server.py          # Main MCP server
│       ├── connections.py     # Database connection management
│       ├── safety.py          # SQL safety checker
│       └── dialects.py        # Platform-specific SQL helpers
├── tests/                     # Test files
├── pyproject.toml            # Project configuration
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/
ruff check src/
```

## Contributing

Contributions are welcome! Please ensure:
- Code follows the existing style (use black and ruff)
- Tests are added for new features
- Documentation is updated

## License

MIT License

## Support

For issues, questions, or contributions, please open an issue on GitHub.