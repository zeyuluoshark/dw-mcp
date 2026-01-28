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

Configure database connections using environment variables:

```bash
# MaxCompute (example format - adjust based on actual connector)
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

**Note**: Not all platforms need to be configured. The server will only enable tools for platforms with valid connection strings.

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