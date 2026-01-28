#!/usr/bin/env python3
"""Data Warehouse MCP Server - Multi-platform SQL assistant."""

import asyncio
import json
from typing import Any
from mcp.server import Server
from mcp.types import (
    Tool,
    TextContent,
    GetPromptResult,
    Prompt,
    PromptArgument,
    PromptMessage,
)
import mcp.server.stdio

from .connections import ConnectionManager
from .safety import SQLSafetyChecker
from .dialects import SQLDialectHelper

# Initialize connection manager
conn_manager = ConnectionManager()

# Create MCP server
app = Server("dw-mcp")


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    """List available prompt templates."""
    return [
        Prompt(
            name="explain-schema",
            description="Explain the schema and structure of a table",
            arguments=[
                PromptArgument(
                    name="platform",
                    description="Platform name (maxcompute, hologres, mysql, polardb, redshift)",
                    required=True,
                ),
                PromptArgument(
                    name="table",
                    description="Table name to explain",
                    required=True,
                ),
            ],
        ),
        Prompt(
            name="data-lineage",
            description="Explain data lineage and dependencies",
            arguments=[
                PromptArgument(
                    name="table",
                    description="Table name to trace lineage",
                    required=True,
                ),
            ],
        ),
        Prompt(
            name="query-optimization",
            description="Get suggestions for optimizing a query",
            arguments=[
                PromptArgument(
                    name="platform",
                    description="Platform name",
                    required=True,
                ),
                PromptArgument(
                    name="query",
                    description="SQL query to optimize",
                    required=True,
                ),
            ],
        ),
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
    """Get a specific prompt template."""
    if name == "explain-schema":
        platform = arguments.get("platform", "") if arguments else ""
        table = arguments.get("table", "") if arguments else ""

        return GetPromptResult(
            description=f"Explaining schema for {table} on {platform}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=f"Please explain the schema and structure of table '{table}' on platform '{platform}'. Include column names, data types, and any constraints or indexes.",
                    ),
                )
            ],
        )

    elif name == "data-lineage":
        table = arguments.get("table", "") if arguments else ""

        return GetPromptResult(
            description=f"Explaining data lineage for {table}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=f"Please explain the data lineage for table '{table}'. Show upstream sources and downstream dependencies.",
                    ),
                )
            ],
        )

    elif name == "query-optimization":
        platform = arguments.get("platform", "") if arguments else ""
        query = arguments.get("query", "") if arguments else ""

        return GetPromptResult(
            description=f"Optimizing query for {platform}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=f"Please analyze this query for platform '{platform}' and suggest optimizations:\n\n{query}",
                    ),
                )
            ],
        )

    raise ValueError(f"Unknown prompt: {name}")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="list_platforms",
            description="List all available configured data warehouse platforms",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_platform_info",
            description="Get detailed information about a specific platform (features, dialect, use cases)",
            inputSchema={
                "type": "object",
                "properties": {
                    "platform": {
                        "type": "string",
                        "description": "Platform name (maxcompute, hologres, mysql, polardb, redshift)",
                    },
                },
                "required": ["platform"],
            },
        ),
        Tool(
            name="execute_query",
            description="Execute a SQL query on specified platform. Automatically adds LIMIT for SELECT queries. Destructive operations require explicit confirmation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "platform": {
                        "type": "string",
                        "description": "Platform name (maxcompute, hologres, mysql, polardb, redshift)",
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 100)",
                        "default": 100,
                    },
                    "allow_destructive": {
                        "type": "boolean",
                        "description": "Explicitly allow destructive operations (DELETE, UPDATE, DROP, etc.)",
                        "default": False,
                    },
                },
                "required": ["platform", "query"],
            },
        ),
        Tool(
            name="validate_query",
            description="Validate a SQL query for safety without executing it",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to validate",
                    },
                    "allow_destructive": {
                        "type": "boolean",
                        "description": "Allow destructive operations",
                        "default": False,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_schema_info",
            description="Get schema information (tables and columns) for a platform",
            inputSchema={
                "type": "object",
                "properties": {
                    "platform": {
                        "type": "string",
                        "description": "Platform name",
                    },
                    "schema": {
                        "type": "string",
                        "description": "Optional specific schema name",
                    },
                },
                "required": ["platform"],
            },
        ),
        Tool(
            name="get_example_queries",
            description="Get example queries for a specific platform",
            inputSchema={
                "type": "object",
                "properties": {
                    "platform": {
                        "type": "string",
                        "description": "Platform name",
                    },
                },
                "required": ["platform"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls."""

    if name == "list_platforms":
        platforms = conn_manager.list_available_platforms()

        if not platforms:
            response = {
                "available_platforms": [],
                "message": "No platforms configured. Set environment variables for connections.",
                "env_vars": [
                    "MAXCOMPUTE_CONNECTION",
                    "HOLOGRES_CONNECTION",
                    "MYSQL_CONNECTION",
                    "POLARDB_CONNECTION",
                    "REDSHIFT_CONNECTION",
                ],
            }
        else:
            platform_details = []
            for p in platforms:
                info = SQLDialectHelper.get_platform_info(p)
                platform_details.append(
                    {
                        "platform": p,
                        "name": info.get("name"),
                        "type": info.get("type"),
                        "description": info.get("description"),
                    }
                )

            response = {"available_platforms": platforms, "details": platform_details}

        return [TextContent(type="text", text=json.dumps(response, indent=2))]

    elif name == "get_platform_info":
        platform = arguments.get("platform")
        info = SQLDialectHelper.get_platform_info(platform)

        return [TextContent(type="text", text=json.dumps(info, indent=2))]

    elif name == "execute_query":
        platform = arguments.get("platform")
        query = arguments.get("query")
        limit = arguments.get("limit", 100)
        allow_destructive = arguments.get("allow_destructive", False)

        # Validate query first
        is_valid, processed_query, message = SQLSafetyChecker.validate_query(
            query, allow_destructive=allow_destructive, auto_limit=True, default_limit=limit
        )

        if not is_valid:
            response = {"success": False, "error": message, "query": query}
            return [TextContent(type="text", text=json.dumps(response, indent=2))]

        # Execute query
        result = conn_manager.execute_query(platform, processed_query, limit=None)

        # Format output
        formatted = SQLDialectHelper.format_query_results(result, format_type="table")

        return [
            TextContent(type="text", text=formatted),
            TextContent(
                type="text", text=f"\n\nRaw JSON:\n{json.dumps(result, indent=2, default=str)}"
            ),
        ]

    elif name == "validate_query":
        query = arguments.get("query")
        allow_destructive = arguments.get("allow_destructive", False)

        is_valid, processed_query, message = SQLSafetyChecker.validate_query(
            query, allow_destructive=allow_destructive, auto_limit=True
        )

        response = {
            "valid": is_valid,
            "message": message,
            "original_query": query,
            "processed_query": processed_query if is_valid else None,
            "is_select": SQLSafetyChecker.is_select_query(query),
            "is_destructive": SQLSafetyChecker.is_destructive(query)[0],
        }

        return [TextContent(type="text", text=json.dumps(response, indent=2))]

    elif name == "get_schema_info":
        platform = arguments.get("platform")
        schema = arguments.get("schema")

        result = conn_manager.get_schema_info(platform, schema)

        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    elif name == "get_example_queries":
        platform = arguments.get("platform")
        examples = SQLDialectHelper.get_example_queries(platform)

        response = {"platform": platform, "examples": examples}

        return [TextContent(type="text", text=json.dumps(response, indent=2))]

    raise ValueError(f"Unknown tool: {name}")


async def main():
    """Run the MCP server."""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
