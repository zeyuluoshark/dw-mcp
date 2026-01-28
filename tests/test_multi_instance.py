"""Tests for multi-instance configuration support."""

from unittest.mock import patch, MagicMock
from src.dw_mcp.connections import ConnectionManager


class TestMultiInstanceConfiguration:
    """Test multi-instance environment variable configuration."""

    def test_parse_maxcompute_multi_instance(self):
        """Test parsing MaxCompute multi-instance configuration."""
        env_vars = {
            "MAXCOMPUTE_REGION1_PROJECT1_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY": "test_key",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "maxcompute_region1_project1" in configs
            config = configs["maxcompute_region1_project1"]
            assert config["TYPE"] == "MAXCOMPUTE"
            assert config["PROJECT"] == "test_project"
            assert config["ACCESSID"] == "test_id"

    def test_parse_dataworks_multi_instance(self):
        """Test parsing DataWorks multi-instance configuration."""
        env_vars = {
            "DATAWORKS_REGION2_PROJECT2_TYPE": "DATAWORKS",
            "DATAWORKS_REGION2_PROJECT2_PROJECT": "test_project",
            "DATAWORKS_REGION2_PROJECT2_ACCESSID": "test_id",
            "DATAWORKS_REGION2_PROJECT2_ACCESSKEY": "test_key",
            "DATAWORKS_REGION2_PROJECT2_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "dataworks_region2_project2" in configs
            config = configs["dataworks_region2_project2"]
            assert config["TYPE"] == "DATAWORKS"

    def test_parse_hologres_multi_instance(self):
        """Test parsing Hologres multi-instance configuration."""
        env_vars = {
            "HOLO_REGION1_DB1_TYPE": "HOLOGRES",
            "HOLO_REGION1_DB1_HOST": "test-instance.hologres.aliyuncs.com",
            "HOLO_REGION1_DB1_USER": "test_user",
            "HOLO_REGION1_DB1_PASSWORD": "test_pass",
            "HOLO_REGION1_DB1_DBNAME": "test_db",
            "HOLO_REGION1_DB1_PORT": "80",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "holo_region1_db1" in configs
            config = configs["holo_region1_db1"]
            assert config["TYPE"] == "HOLOGRES"
            assert config["HOST"] == "test-instance.hologres.aliyuncs.com"

    def test_parse_mysql_multi_instance(self):
        """Test parsing MySQL multi-instance configuration."""
        env_vars = {
            "MYSQL_REGION1_DB1_TYPE": "MySQL",
            "MYSQL_REGION1_DB1_HOST": "test-instance.rds.aliyuncs.com",
            "MYSQL_REGION1_DB1_USER": "test_user",
            "MYSQL_REGION1_DB1_PASSWORD": "test_pass",
            "MYSQL_REGION1_DB1_DB": "test_db",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "mysql_region1_db1" in configs
            config = configs["mysql_region1_db1"]
            assert config["TYPE"] == "MySQL"

    def test_parse_polardb_multi_instance(self):
        """Test parsing PolarDB multi-instance configuration."""
        env_vars = {
            "POLARDB_REGION1_DB1_TYPE": "POLARDB",
            "POLARDB_REGION1_DB1_HOST": "test-instance.rwlb.rds.aliyuncs.com",
            "POLARDB_REGION1_DB1_USER": "test_user",
            "POLARDB_REGION1_DB1_PASSWORD": "test_pass",
            "POLARDB_REGION1_DB1_DB": "test_db",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "polardb_region1_db1" in configs
            config = configs["polardb_region1_db1"]
            assert config["TYPE"] == "POLARDB"

    def test_parse_redshift_multi_instance(self):
        """Test parsing Redshift multi-instance configuration."""
        env_vars = {
            "REDSHIFT_REGION1_CLUSTER1_TYPE": "REDSHIFT",
            "REDSHIFT_REGION1_CLUSTER1_HOST": "test-workgroup.test-region.redshift-serverless.amazonaws.com",
            "REDSHIFT_REGION1_CLUSTER1_PORT": "5439",
            "REDSHIFT_REGION1_CLUSTER1_DB": "test_db",
            "REDSHIFT_REGION1_CLUSTER1_USER": "test_user",
            "REDSHIFT_REGION1_CLUSTER1_PASSWORD": "test_pass",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "redshift_region1_cluster1" in configs
            config = configs["redshift_region1_cluster1"]
            assert config["TYPE"] == "REDSHIFT"

    def test_build_maxcompute_connection_string(self):
        """Test building MaxCompute connection string."""
        config = {
            "TYPE": "MAXCOMPUTE",
            "PROJECT": "test_project",
            "ACCESSID": "test_id",
            "ACCESSKEY": "test_key",
            "ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        assert conn_string.startswith("maxcompute://")
        assert "test_id" in conn_string
        assert "test_key" in conn_string
        assert "test_project" in conn_string

    def test_build_dataworks_connection_string(self):
        """Test building DataWorks connection string (maps to MaxCompute)."""
        config = {
            "TYPE": "DATAWORKS",
            "PROJECT": "test_project",
            "ACCESSID": "test_id",
            "ACCESSKEY": "test_key",
            "ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        assert conn_string.startswith("maxcompute://")

    def test_build_hologres_connection_string(self):
        """Test building Hologres connection string."""
        config = {
            "TYPE": "HOLOGRES",
            "HOST": "test-instance.hologres.aliyuncs.com",
            "USER": "test_user",
            "PASSWORD": "test_pass",
            "DBNAME": "test_db",
            "PORT": "80",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        assert conn_string.startswith("postgresql://")
        assert "test_db" in conn_string

    def test_build_mysql_connection_string(self):
        """Test building MySQL connection string."""
        config = {
            "TYPE": "MySQL",
            "HOST": "test-instance.rds.aliyuncs.com",
            "USER": "test_user",
            "PASSWORD": "test_pass",
            "DB": "test_db",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        assert conn_string.startswith("mysql+pymysql://")
        assert "test_db" in conn_string

    def test_build_polardb_connection_string(self):
        """Test building PolarDB connection string."""
        config = {
            "TYPE": "POLARDB",
            "HOST": "test-instance.rwlb.rds.aliyuncs.com",
            "USER": "test_user",
            "PASSWORD": "test_pass",
            "DB": "test_db",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        assert conn_string.startswith("mysql+pymysql://")

    def test_build_redshift_connection_string(self):
        """Test building Redshift connection string."""
        config = {
            "TYPE": "REDSHIFT",
            "HOST": "test-workgroup.test-region.redshift-serverless.amazonaws.com",
            "PORT": "5439",
            "DB": "test_db",
            "USER": "test_user",
            "PASSWORD": "test_pass",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        assert conn_string.startswith("redshift+redshift_connector://")

    def test_special_characters_in_credentials(self):
        """Test handling special characters in credentials."""
        config = {
            "TYPE": "HOLOGRES",
            "HOST": "test.host.com",
            "USER": "BASIC$user",
            "PASSWORD": "pass$word@123",
            "DBNAME": "testdb",
            "PORT": "80",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("test_instance", config)
        
        assert conn_string is not None
        # URL-encoded special characters should be in the connection string
        assert "BASIC%24user" in conn_string or "BASIC$user" in conn_string

    def test_mixed_legacy_and_multi_instance(self):
        """Test using both legacy and multi-instance formats together."""
        env_vars = {
            # Legacy format
            "MYSQL_CONNECTION": "mysql+pymysql://user:pass@host/db",
            # Multi-instance format
            "MAXCOMPUTE_REGION1_PROJECT1_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY": "test_key",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                
                manager = ConnectionManager()
                platforms = manager.list_available_platforms()
                
                # Should have both legacy mysql and new maxcompute instance
                assert "mysql" in platforms
                assert "maxcompute_region1_project1" in platforms

    def test_incomplete_configuration_ignored(self):
        """Test that incomplete configurations are ignored."""
        env_vars = {
            # Missing ACCESSKEY
            "MAXCOMPUTE_REGION1_PROJECT1_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                
                manager = ConnectionManager()
                
                # Should not create engine for incomplete config
                # Check that build_connection_string returns None
                configs = manager._parse_multi_instance_configs()
                conn_string = manager._build_connection_string("maxcompute_region1_project1", configs["maxcompute_region1_project1"])
                assert conn_string is None

    def test_multiple_instances_same_platform(self):
        """Test multiple instances of the same platform type."""
        env_vars = {
            # First MaxCompute instance
            "MAXCOMPUTE_REGION1_PROJECT1_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project_1",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id_1",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY": "test_key_1",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region-1.maxcompute.aliyun.com/api",
            # Second MaxCompute instance
            "MAXCOMPUTE_REGION2_PROJECT2_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION2_PROJECT2_PROJECT": "test_project_2",
            "MAXCOMPUTE_REGION2_PROJECT2_ACCESSID": "test_id_2",
            "MAXCOMPUTE_REGION2_PROJECT2_ACCESSKEY": "test_key_2",
            "MAXCOMPUTE_REGION2_PROJECT2_ENDPOINT": "http://service.test-region-2.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                
                manager = ConnectionManager()
                platforms = manager.list_available_platforms()
                
                # Should have both instances
                assert "maxcompute_region1_project1" in platforms
                assert "maxcompute_region2_project2" in platforms

    def test_ignores_invalid_platform_types(self):
        """Test that environment variables with invalid platform types are ignored."""
        env_vars = {
            # Valid platform
            "MAXCOMPUTE_REGION1_PROJECT1_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY": "test_key",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
            # Invalid platform type (not in VALID_TYPES)
            "INVALID_REGION_TEST_TYPE": "INVALID",
            "INVALID_REGION_TEST_PARAM": "value",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            # Should only parse the valid maxcompute instance
            assert "maxcompute_region1_project1" in configs
            assert "invalid_region_test" not in configs

    def test_requires_type_parameter(self):
        """Test that instances without TYPE parameter are not returned."""
        env_vars = {
            # Missing TYPE parameter
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY": "test_key",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            # Should not return instance without TYPE parameter
            assert "maxcompute_region1_project1" not in configs

    def test_error_handling_continues_loading(self):
        """Test that errors in one instance don't prevent loading others."""
        env_vars = {
            # First instance (will fail)
            "MAXCOMPUTE_REGION1_PROJECT1_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_REGION1_PROJECT1_PROJECT": "test_project",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSID": "test_id",
            "MAXCOMPUTE_REGION1_PROJECT1_ACCESSKEY": "test_key",
            "MAXCOMPUTE_REGION1_PROJECT1_ENDPOINT": "http://service.test-region.maxcompute.aliyun.com/api",
            # Second instance (will succeed)
            "MYSQL_REGION1_TEST_TYPE": "MySQL",
            "MYSQL_REGION1_TEST_HOST": "localhost",
            "MYSQL_REGION1_TEST_USER": "test_user",
            "MYSQL_REGION1_TEST_PASSWORD": "test_pass",
            "MYSQL_REGION1_TEST_DB": "test_db",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                # First call raises error, second succeeds
                mock_create_engine.side_effect = [Exception("Test error"), MagicMock()]
                
                manager = ConnectionManager()
                platforms = manager.list_available_platforms()
                
                # MySQL instance should succeed even though MaxCompute failed
                assert "mysql_region1_test" in platforms
