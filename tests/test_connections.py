"""Tests for connection manager."""

from unittest.mock import patch
from src.dw_mcp.connections import ConnectionManager, Platform


class TestConnectionManager:
    """Test the connection manager."""

    def test_list_available_platforms_empty(self):
        """Test listing platforms when none are configured."""
        with patch.dict("os.environ", {}, clear=True):
            manager = ConnectionManager()
            platforms = manager.list_available_platforms()
            assert platforms == []

    def test_list_available_platforms(self):
        """Test listing configured platforms."""
        env_vars = {
            "MYSQL_CONNECTION": "mysql+pymysql://user:pass@host/db",
            "HOLOGRES_CONNECTION": "postgresql://user:pass@host/db",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            platforms = manager.list_available_platforms()
            assert Platform.MYSQL in platforms
            assert Platform.HOLOGRES in platforms

    def test_get_engine_not_configured(self):
        """Test getting engine for non-configured platform."""
        with patch.dict("os.environ", {}, clear=True):
            manager = ConnectionManager()
            engine = manager.get_engine(Platform.MYSQL)
            assert engine is None

    def test_execute_query_platform_not_configured(self):
        """Test executing query on non-configured platform."""
        with patch.dict("os.environ", {}, clear=True):
            manager = ConnectionManager()
            result = manager.execute_query(Platform.MYSQL, "SELECT 1")

            assert result["success"] is False
            assert "not configured" in result["error"]
