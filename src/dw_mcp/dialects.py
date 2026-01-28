"""SQL dialect helpers for different data warehouse platforms."""

from typing import Dict, Any
from .connections import Platform


class SQLDialectHelper:
    """Provides platform-specific SQL dialect information and helpers."""

    @staticmethod
    def get_platform_info(platform: str) -> Dict[str, Any]:
        """
        Get information about a specific platform.

        Args:
            platform: Platform name

        Returns:
            Dictionary with platform information
        """
        platform_info = {
            Platform.MAXCOMPUTE: {
                "name": "MaxCompute",
                "type": "Offline Data Warehouse",
                "description": "Alibaba Cloud MaxCompute for offline DW tables and batch processing",
                "dialect": "MaxCompute SQL (similar to Hive)",
                "use_cases": ["Offline analytics", "Batch processing", "Data warehouse"],
                "features": [
                    "Partitioned tables",
                    "Distributed processing",
                    "UDF support",
                    "Cost-based optimization",
                ],
                "common_functions": [
                    "WCOUNT() - Word count",
                    "GET_JSON_OBJECT() - Parse JSON",
                    "CONCAT_WS() - Concatenate with separator",
                    "TO_DATE() - Date conversion",
                ],
            },
            Platform.HOLOGRES: {
                "name": "Hologres",
                "type": "Real-time Analytics",
                "description": "Alibaba Cloud Hologres for real-time analytics and OLAP",
                "dialect": "PostgreSQL-compatible",
                "use_cases": ["Real-time analytics", "OLAP", "Interactive queries"],
                "features": [
                    "PostgreSQL compatible",
                    "Real-time data serving",
                    "High-performance queries",
                    "Row and column storage",
                ],
                "common_functions": [
                    "Standard PostgreSQL functions",
                    "Window functions",
                    "JSON functions",
                    "Array functions",
                ],
            },
            Platform.MYSQL: {
                "name": "MySQL",
                "type": "Source System",
                "description": "MySQL database for source systems and transactional data",
                "dialect": "MySQL",
                "use_cases": ["OLTP", "Application databases", "Source data"],
                "features": [
                    "ACID transactions",
                    "Stored procedures",
                    "Triggers",
                    "Full-text search",
                ],
                "common_functions": [
                    "NOW() - Current timestamp",
                    "CONCAT() - String concatenation",
                    "DATE_FORMAT() - Format dates",
                    "IFNULL() - Null handling",
                ],
            },
            Platform.POLARDB: {
                "name": "PolarDB",
                "type": "Source System",
                "description": "Alibaba Cloud PolarDB for MySQL-compatible source systems",
                "dialect": "MySQL-compatible",
                "use_cases": ["OLTP", "High-performance databases", "Source data"],
                "features": [
                    "MySQL compatible",
                    "High performance",
                    "Distributed storage",
                    "Read replicas",
                ],
                "common_functions": [
                    "MySQL-compatible functions",
                    "JSON functions",
                    "Full-text search",
                    "GIS functions",
                ],
            },
            Platform.REDSHIFT: {
                "name": "Redshift",
                "type": "Regional Data Warehouse",
                "description": "AWS Redshift for EU data and regional analytics",
                "dialect": "PostgreSQL-based",
                "use_cases": ["Data warehouse", "Regional analytics", "EU data"],
                "features": [
                    "Columnar storage",
                    "Massively parallel processing",
                    "Distribution keys",
                    "Sort keys",
                ],
                "common_functions": [
                    "LISTAGG() - String aggregation",
                    "MEDIAN() - Median calculation",
                    "PERCENTILE_CONT() - Percentiles",
                    "JSON_EXTRACT_PATH_TEXT() - JSON parsing",
                ],
            },
        }

        return platform_info.get(
            platform,
            {
                "name": platform,
                "type": "Unknown",
                "description": "Unknown platform",
                "dialect": "SQL",
                "use_cases": [],
                "features": [],
                "common_functions": [],
            },
        )

    @staticmethod
    def get_example_queries(platform: str) -> list[Dict[str, str]]:
        """
        Get example queries for a platform.

        Args:
            platform: Platform name

        Returns:
            List of example query dictionaries
        """
        examples = {
            Platform.MAXCOMPUTE: [
                {"description": "List all tables in a project", "query": "SHOW TABLES;"},
                {"description": "Describe table structure", "query": "DESC table_name;"},
                {
                    "description": "Query with partition",
                    "query": "SELECT * FROM table_name WHERE ds='20240101' LIMIT 10;",
                },
                {
                    "description": "Count rows in table",
                    "query": "SELECT COUNT(*) as row_count FROM table_name;",
                },
            ],
            Platform.HOLOGRES: [
                {
                    "description": "List all tables in schema",
                    "query": "SELECT tablename FROM pg_tables WHERE schemaname='public';",
                },
                {
                    "description": "Describe table columns",
                    "query": "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='table_name';",
                },
                {
                    "description": "Sample data from table",
                    "query": "SELECT * FROM table_name LIMIT 10;",
                },
                {
                    "description": "Aggregate query",
                    "query": "SELECT category, COUNT(*) as cnt FROM table_name GROUP BY category LIMIT 100;",
                },
            ],
            Platform.MYSQL: [
                {"description": "Show all tables", "query": "SHOW TABLES;"},
                {"description": "Describe table structure", "query": "DESCRIBE table_name;"},
                {
                    "description": "Sample recent data",
                    "query": "SELECT * FROM table_name ORDER BY created_at DESC LIMIT 10;",
                },
                {
                    "description": "Count by category",
                    "query": "SELECT category, COUNT(*) as count FROM table_name GROUP BY category;",
                },
            ],
            Platform.POLARDB: [
                {"description": "Show databases", "query": "SHOW DATABASES;"},
                {"description": "Show tables", "query": "SHOW TABLES;"},
                {"description": "Table structure", "query": "SHOW CREATE TABLE table_name;"},
                {
                    "description": "Recent records",
                    "query": "SELECT * FROM table_name ORDER BY id DESC LIMIT 10;",
                },
            ],
            Platform.REDSHIFT: [
                {
                    "description": "List tables in schema",
                    "query": "SELECT tablename FROM pg_tables WHERE schemaname='public';",
                },
                {
                    "description": "Table column details",
                    "query": "SELECT * FROM information_schema.columns WHERE table_name='table_name' LIMIT 100;",
                },
                {
                    "description": "Distribution and sort keys",
                    "query": "SELECT * FROM pg_table_def WHERE tablename='table_name';",
                },
                {"description": "Sample data", "query": "SELECT * FROM table_name LIMIT 10;"},
            ],
        }

        return examples.get(platform, [])

    @staticmethod
    def format_query_results(results: Dict[str, Any], format_type: str = "table") -> str:
        """
        Format query results for display.

        Args:
            results: Query results dictionary
            format_type: Format type (table, json, csv)

        Returns:
            Formatted string
        """
        if not results.get("success"):
            return f"Error: {results.get('error', 'Unknown error')}"

        if format_type == "json":
            import json

            return json.dumps(results, indent=2, default=str)

        if not results.get("rows"):
            return results.get("message", "Query executed successfully with no results")

        # Table format
        columns = results.get("columns", [])
        rows = results.get("rows", [])

        if not columns or not rows:
            return "No data returned"

        # Calculate column widths
        col_widths = {col: len(col) for col in columns}
        for row in rows:
            for col in columns:
                val_len = len(str(row.get(col, "")))
                col_widths[col] = max(col_widths[col], val_len)

        # Build table
        header = " | ".join(col.ljust(col_widths[col]) for col in columns)
        separator = "-+-".join("-" * col_widths[col] for col in columns)

        lines = [header, separator]
        for row in rows:
            line = " | ".join(str(row.get(col, "")).ljust(col_widths[col]) for col in columns)
            lines.append(line)

        return "\n".join(lines) + f"\n\n({len(rows)} rows)"
