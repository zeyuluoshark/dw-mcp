"""Tests for SQL safety checker."""

import pytest
from src.dw_mcp.safety import SQLSafetyChecker


class TestSQLSafetyChecker:
    """Test the SQL safety checker."""

    def test_is_select_query(self):
        """Test SELECT query detection."""
        assert SQLSafetyChecker.is_select_query("SELECT * FROM table")
        assert SQLSafetyChecker.is_select_query("select * from table")
        assert SQLSafetyChecker.is_select_query("  SELECT * FROM table  ")
        assert SQLSafetyChecker.is_select_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert SQLSafetyChecker.is_select_query("SHOW TABLES")
        assert SQLSafetyChecker.is_select_query("DESCRIBE table_name")
        assert SQLSafetyChecker.is_select_query("EXPLAIN SELECT * FROM table")
        
        assert not SQLSafetyChecker.is_select_query("INSERT INTO table VALUES (1)")
        assert not SQLSafetyChecker.is_select_query("UPDATE table SET col=1")
        assert not SQLSafetyChecker.is_select_query("DELETE FROM table")

    def test_is_destructive(self):
        """Test destructive operation detection."""
        # Destructive operations
        destructive_queries = [
            "DROP TABLE users",
            "drop table users",
            "TRUNCATE TABLE logs",
            "DELETE FROM users WHERE id=1",
            "UPDATE users SET name='test' WHERE id=1",
            "INSERT INTO users VALUES (1, 'test')",
            "CREATE TABLE new_table (id INT)",
            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
            "DROP DATABASE mydb",
        ]
        
        for query in destructive_queries:
            is_dest, patterns = SQLSafetyChecker.is_destructive(query)
            assert is_dest, f"Query should be destructive: {query}"
            assert len(patterns) > 0
        
        # Non-destructive operations
        safe_queries = [
            "SELECT * FROM users",
            "SHOW TABLES",
            "DESCRIBE users",
            "EXPLAIN SELECT * FROM users",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
        ]
        
        for query in safe_queries:
            is_dest, patterns = SQLSafetyChecker.is_destructive(query)
            assert not is_dest, f"Query should be safe: {query}"
            assert len(patterns) == 0

    def test_suggest_limit(self):
        """Test automatic LIMIT addition."""
        # Should add LIMIT
        query = "SELECT * FROM users"
        result = SQLSafetyChecker.suggest_limit(query, 100)
        assert "LIMIT 100" in result
        
        # Should not add LIMIT if already present
        query = "SELECT * FROM users LIMIT 50"
        result = SQLSafetyChecker.suggest_limit(query, 100)
        assert result == query
        
        # Should not add LIMIT to non-SELECT queries
        query = "UPDATE users SET name='test'"
        result = SQLSafetyChecker.suggest_limit(query, 100)
        assert "LIMIT" not in result

    def test_validate_query(self):
        """Test query validation."""
        # Valid SELECT query
        is_valid, processed, msg = SQLSafetyChecker.validate_query(
            "SELECT * FROM users",
            allow_destructive=False
        )
        assert is_valid
        assert "LIMIT" in processed
        assert "successfully" in msg.lower()
        
        # Invalid destructive query without permission
        is_valid, processed, msg = SQLSafetyChecker.validate_query(
            "DELETE FROM users",
            allow_destructive=False
        )
        assert not is_valid
        assert "destructive" in msg.lower()
        
        # Valid destructive query with permission
        is_valid, processed, msg = SQLSafetyChecker.validate_query(
            "DELETE FROM users WHERE id=1",
            allow_destructive=True
        )
        assert is_valid
        
        # Empty query
        is_valid, processed, msg = SQLSafetyChecker.validate_query("")
        assert not is_valid
        assert "empty" in msg.lower()
