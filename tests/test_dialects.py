"""Tests for SQL dialect helpers."""

from src.dw_mcp.dialects import SQLDialectHelper
from src.dw_mcp.connections import Platform


class TestSQLDialectHelper:
    """Test SQL dialect helper functions."""

    def test_get_platform_info(self):
        """Test getting platform information."""
        # Test known platforms
        for platform in [
            Platform.MAXCOMPUTE,
            Platform.HOLOGRES,
            Platform.MYSQL,
            Platform.POLARDB,
            Platform.REDSHIFT,
        ]:
            info = SQLDialectHelper.get_platform_info(platform)

            assert "name" in info
            assert "type" in info
            assert "description" in info
            assert "dialect" in info
            assert "use_cases" in info
            assert "features" in info
            assert "common_functions" in info
            assert len(info["features"]) > 0

        # Test unknown platform
        info = SQLDialectHelper.get_platform_info("unknown")
        assert info["type"] == "Unknown"

    def test_get_example_queries(self):
        """Test getting example queries."""
        for platform in [
            Platform.MAXCOMPUTE,
            Platform.HOLOGRES,
            Platform.MYSQL,
            Platform.POLARDB,
            Platform.REDSHIFT,
        ]:
            examples = SQLDialectHelper.get_example_queries(platform)

            assert isinstance(examples, list)
            assert len(examples) > 0

            for example in examples:
                assert "description" in example
                assert "query" in example
                assert isinstance(example["query"], str)

    def test_format_query_results(self):
        """Test formatting query results."""
        # Test successful query with results
        results = {
            "success": True,
            "columns": ["id", "name", "age"],
            "rows": [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
        }

        formatted = SQLDialectHelper.format_query_results(results, "table")
        assert "id" in formatted
        assert "name" in formatted
        assert "Alice" in formatted
        assert "Bob" in formatted
        assert "(2 rows)" in formatted

        # Test error result
        results = {"success": False, "error": "Connection failed"}

        formatted = SQLDialectHelper.format_query_results(results, "table")
        assert "Error" in formatted
        assert "Connection failed" in formatted

        # Test JSON format
        results = {
            "success": True,
            "columns": ["id"],
            "rows": [{"id": 1}],
        }

        formatted = SQLDialectHelper.format_query_results(results, "json")
        assert '"success": true' in formatted.lower()
