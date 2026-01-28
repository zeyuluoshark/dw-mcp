"""Database connection management for different data platforms."""

import os
from typing import Optional, Dict, Any
from enum import Enum
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class Platform(str, Enum):
    """Supported data warehouse platforms."""
    MAXCOMPUTE = "maxcompute"
    HOLOGRES = "hologres"
    MYSQL = "mysql"
    POLARDB = "polardb"
    REDSHIFT = "redshift"


class ConnectionManager:
    """Manages connections to different data warehouse platforms."""

    def __init__(self):
        self._engines: Dict[str, Engine] = {}
        self._load_connections()

    def _load_connections(self):
        """Load connection strings from environment variables."""
        # MaxCompute connection
        if os.getenv("MAXCOMPUTE_CONNECTION"):
            self._engines[Platform.MAXCOMPUTE] = create_engine(
                os.getenv("MAXCOMPUTE_CONNECTION"),
                echo=False
            )

        # Hologres connection (PostgreSQL compatible)
        if os.getenv("HOLOGRES_CONNECTION"):
            self._engines[Platform.HOLOGRES] = create_engine(
                os.getenv("HOLOGRES_CONNECTION"),
                echo=False
            )

        # MySQL connection
        if os.getenv("MYSQL_CONNECTION"):
            self._engines[Platform.MYSQL] = create_engine(
                os.getenv("MYSQL_CONNECTION"),
                echo=False
            )

        # PolarDB connection (MySQL compatible)
        if os.getenv("POLARDB_CONNECTION"):
            self._engines[Platform.POLARDB] = create_engine(
                os.getenv("POLARDB_CONNECTION"),
                echo=False
            )

        # Redshift connection
        if os.getenv("REDSHIFT_CONNECTION"):
            self._engines[Platform.REDSHIFT] = create_engine(
                os.getenv("REDSHIFT_CONNECTION"),
                echo=False
            )

    def get_engine(self, platform: str) -> Optional[Engine]:
        """Get database engine for specified platform."""
        return self._engines.get(platform)

    def list_available_platforms(self) -> list[str]:
        """List all available configured platforms."""
        return list(self._engines.keys())

    def execute_query(self, platform: str, query: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a query on the specified platform.
        
        Args:
            platform: Target platform name
            query: SQL query to execute
            limit: Optional row limit to apply
            
        Returns:
            Dictionary containing results and metadata
        """
        engine = self.get_engine(platform)
        if not engine:
            return {
                "success": False,
                "error": f"Platform '{platform}' not configured. Available: {self.list_available_platforms()}"
            }

        try:
            # Add LIMIT if not present and limit is specified
            query_to_execute = query.strip()
            if limit and "LIMIT" not in query_to_execute.upper():
                query_to_execute = f"{query_to_execute.rstrip(';')} LIMIT {limit}"

            with engine.connect() as conn:
                result = conn.execute(text(query_to_execute))
                
                # Check if this is a SELECT query
                if result.returns_rows:
                    rows = result.fetchall()
                    columns = list(result.keys())
                    
                    return {
                        "success": True,
                        "platform": platform,
                        "columns": columns,
                        "rows": [dict(zip(columns, row)) for row in rows],
                        "row_count": len(rows),
                        "query": query_to_execute
                    }
                else:
                    # For non-SELECT queries, return execution info
                    return {
                        "success": True,
                        "platform": platform,
                        "message": "Query executed successfully (non-SELECT)",
                        "rowcount": result.rowcount if hasattr(result, 'rowcount') else None,
                        "query": query_to_execute
                    }
                    
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "platform": platform,
                "query": query_to_execute if 'query_to_execute' in locals() else query
            }

    def get_schema_info(self, platform: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get schema information for the specified platform.
        
        Args:
            platform: Target platform name
            schema: Optional specific schema name
            
        Returns:
            Dictionary containing schema metadata
        """
        engine = self.get_engine(platform)
        if not engine:
            return {
                "success": False,
                "error": f"Platform '{platform}' not configured"
            }

        try:
            inspector = sqlalchemy.inspect(engine)
            
            schemas = [schema] if schema else inspector.get_schema_names()
            schema_info = {}
            
            for schema_name in schemas:
                tables = inspector.get_table_names(schema=schema_name)
                schema_info[schema_name] = {
                    "tables": []
                }
                
                for table in tables:
                    columns = inspector.get_columns(table, schema=schema_name)
                    schema_info[schema_name]["tables"].append({
                        "name": table,
                        "columns": [
                            {
                                "name": col["name"],
                                "type": str(col["type"]),
                                "nullable": col.get("nullable", True)
                            }
                            for col in columns
                        ]
                    })
            
            return {
                "success": True,
                "platform": platform,
                "schemas": schema_info
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "platform": platform
            }
