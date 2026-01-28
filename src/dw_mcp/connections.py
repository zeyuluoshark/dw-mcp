"""Database connection management for different data platforms."""

import os
import re
from typing import Optional, Dict, Any
from enum import Enum
from urllib.parse import quote_plus
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
    DATAWORKS = "dataworks"  # DataWorks maps to MaxCompute


class ConnectionManager:
    """Manages connections to different data warehouse platforms."""

    def __init__(self):
        self._engines: Dict[str, Engine] = {}
        self._load_connections()

    def _parse_multi_instance_configs(self) -> Dict[str, Dict[str, str]]:
        """
        Parse environment variables with pattern {TYPE}_{REGION}_{PROJECT}_{PARAM}.
        
        Returns:
            Dictionary mapping instance keys to their configuration parameters.
            Example: {'maxcompute_hk_bdw': {'TYPE': 'MAXCOMPUTE', 'PROJECT': 'bit_data_warehouse', ...}}
        """
        configs = {}
        env_vars = os.environ
        
        # Pattern: {TYPE}_{REGION}_{PROJECT}_{PARAM}
        # Examples: MAXCOMPUTE_HK_BDW_TYPE, DATAWORKS_EU_AVBU_PROJECT, POLARDB_CN_INSTA360_DB etc.
        # Type: uppercase letters
        # Region: uppercase letters
        # Project: uppercase letters, digits, underscores (but not ending with underscore before param)
        # Param: uppercase letters and underscores
        pattern = re.compile(r'^([A-Z]+)_([A-Z0-9]+)_([A-Z0-9_]+?)_([A-Z_]+)$')
        
        for key, value in env_vars.items():
            match = pattern.match(key)
            if match:
                type_prefix, region, project, param = match.groups()
                
                # Create instance key: lowercase type_region_project
                instance_key = f"{type_prefix.lower()}_{region.lower()}_{project.lower()}"
                
                if instance_key not in configs:
                    configs[instance_key] = {}
                
                configs[instance_key][param] = value
                
                # Also store the type prefix for platform mapping
                if param == 'TYPE':
                    configs[instance_key]['_TYPE_PREFIX'] = type_prefix
                    configs[instance_key]['_REGION'] = region
                    configs[instance_key]['_PROJECT_KEY'] = project
        
        return configs

    def _build_connection_string(self, instance_key: str, config: Dict[str, str]) -> Optional[str]:
        """
        Build a connection string from configuration parameters.
        
        Args:
            instance_key: Instance identifier (e.g., 'maxcompute_hk_bdw')
            config: Configuration dictionary with parameters
            
        Returns:
            Connection string or None if invalid configuration
        """
        platform_type = config.get('TYPE', '').upper()
        
        if platform_type in ('MAXCOMPUTE', 'DATAWORKS'):
            # MaxCompute/DataWorks format: maxcompute://access_id:access_key@endpoint/project
            access_id = config.get('ACCESSID')
            access_key = config.get('ACCESSKEY')
            project = config.get('PROJECT')
            endpoint = config.get('ENDPOINT')
            
            if access_id and access_key and project and endpoint:
                # URL encode credentials to handle special characters
                encoded_id = quote_plus(access_id)
                encoded_key = quote_plus(access_key)
                # Remove http:// or https:// from endpoint for the connection string
                endpoint_clean = endpoint.replace('http://', '').replace('https://', '')
                return f"maxcompute://{encoded_id}:{encoded_key}@{endpoint_clean}/{project}"
        
        elif platform_type == 'HOLOGRES':
            # Hologres format: postgresql://user:password@host:port/database
            host = config.get('HOST')
            user = config.get('USER')
            password = config.get('PASSWORD')
            db = config.get('DBNAME') or config.get('DB')
            port = config.get('PORT', '80')
            
            if host and user and password and db:
                # URL encode credentials
                encoded_user = quote_plus(user)
                encoded_pass = quote_plus(password)
                return f"postgresql://{encoded_user}:{encoded_pass}@{host}:{port}/{db}"
        
        elif platform_type in ('MYSQL', 'POLARDB'):
            # MySQL/PolarDB format: mysql+pymysql://user:password@host:port/database
            host = config.get('HOST')
            user = config.get('USER')
            password = config.get('PASSWORD')
            db = config.get('DB')
            port = config.get('PORT', '3306')
            
            if host and user and password and db:
                encoded_user = quote_plus(user)
                encoded_pass = quote_plus(password)
                return f"mysql+pymysql://{encoded_user}:{encoded_pass}@{host}:{port}/{db}"
        
        elif platform_type == 'REDSHIFT':
            # Redshift format: redshift+redshift_connector://user:password@host:port/database
            host = config.get('HOST')
            user = config.get('USER')
            password = config.get('PASSWORD')
            db = config.get('DB')
            port = config.get('PORT', '5439')
            
            if host and user and password and db:
                encoded_user = quote_plus(user)
                encoded_pass = quote_plus(password)
                return f"redshift+redshift_connector://{encoded_user}:{encoded_pass}@{host}:{port}/{db}"
        
        return None

    def _load_connections(self):
        """Load connection strings from environment variables."""
        # Load legacy format (single CONNECTION env vars)
        # MaxCompute connection
        if os.getenv("MAXCOMPUTE_CONNECTION"):
            self._engines[Platform.MAXCOMPUTE] = create_engine(
                os.getenv("MAXCOMPUTE_CONNECTION"), echo=False
            )

        # Hologres connection (PostgreSQL compatible)
        if os.getenv("HOLOGRES_CONNECTION"):
            self._engines[Platform.HOLOGRES] = create_engine(
                os.getenv("HOLOGRES_CONNECTION"), echo=False
            )

        # MySQL connection
        if os.getenv("MYSQL_CONNECTION"):
            self._engines[Platform.MYSQL] = create_engine(os.getenv("MYSQL_CONNECTION"), echo=False)

        # PolarDB connection (MySQL compatible)
        if os.getenv("POLARDB_CONNECTION"):
            self._engines[Platform.POLARDB] = create_engine(
                os.getenv("POLARDB_CONNECTION"), echo=False
            )

        # Redshift connection
        if os.getenv("REDSHIFT_CONNECTION"):
            self._engines[Platform.REDSHIFT] = create_engine(
                os.getenv("REDSHIFT_CONNECTION"), echo=False
            )
        
        # Load new format (multi-instance configs)
        multi_configs = self._parse_multi_instance_configs()
        
        for instance_key, config in multi_configs.items():
            conn_string = self._build_connection_string(instance_key, config)
            
            if conn_string:
                try:
                    self._engines[instance_key] = create_engine(conn_string, echo=False)
                except Exception as e:
                    # Log error but continue loading other connections
                    print(f"Warning: Failed to create engine for {instance_key}: {e}")

    def get_engine(self, platform: str) -> Optional[Engine]:
        """Get database engine for specified platform."""
        return self._engines.get(platform)

    def list_available_platforms(self) -> list[str]:
        """List all available configured platforms."""
        return list(self._engines.keys())

    def execute_query(
        self, platform: str, query: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
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
                "error": f"Platform '{platform}' not configured. Available: {self.list_available_platforms()}",
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
                        "query": query_to_execute,
                    }
                else:
                    # For non-SELECT queries, return execution info
                    return {
                        "success": True,
                        "platform": platform,
                        "message": "Query executed successfully (non-SELECT)",
                        "rowcount": result.rowcount if hasattr(result, "rowcount") else None,
                        "query": query_to_execute,
                    }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "platform": platform,
                "query": query_to_execute if "query_to_execute" in locals() else query,
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
            return {"success": False, "error": f"Platform '{platform}' not configured"}

        try:
            inspector = sqlalchemy.inspect(engine)

            schemas = [schema] if schema else inspector.get_schema_names()
            schema_info = {}

            for schema_name in schemas:
                tables = inspector.get_table_names(schema=schema_name)
                schema_info[schema_name] = {"tables": []}

                for table in tables:
                    columns = inspector.get_columns(table, schema=schema_name)
                    schema_info[schema_name]["tables"].append(
                        {
                            "name": table,
                            "columns": [
                                {
                                    "name": col["name"],
                                    "type": str(col["type"]),
                                    "nullable": col.get("nullable", True),
                                }
                                for col in columns
                            ],
                        }
                    )

            return {"success": True, "platform": platform, "schemas": schema_info}

        except Exception as e:
            return {"success": False, "error": str(e), "platform": platform}
