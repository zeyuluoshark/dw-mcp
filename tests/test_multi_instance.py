"""Tests for multi-instance configuration support."""

from unittest.mock import patch, MagicMock
from src.dw_mcp.connections import ConnectionManager


class TestMultiInstanceConfiguration:
    """Test multi-instance environment variable configuration."""

    def test_parse_maxcompute_multi_instance(self):
        """Test parsing MaxCompute multi-instance configuration."""
        env_vars = {
            "MAXCOMPUTE_HK_BDW_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_HK_BDW_PROJECT": "bit_data_warehouse",
            "MAXCOMPUTE_HK_BDW_ACCESSID": "test_id",
            "MAXCOMPUTE_HK_BDW_ACCESSKEY": "test_key",
            "MAXCOMPUTE_HK_BDW_ENDPOINT": "http://service.cn-hongkong.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "maxcompute_hk_bdw" in configs
            config = configs["maxcompute_hk_bdw"]
            assert config["TYPE"] == "MAXCOMPUTE"
            assert config["PROJECT"] == "bit_data_warehouse"
            assert config["ACCESSID"] == "test_id"

    def test_parse_dataworks_multi_instance(self):
        """Test parsing DataWorks multi-instance configuration."""
        env_vars = {
            "DATAWORKS_EU_AVBU_TYPE": "DATAWORKS",
            "DATAWORKS_EU_AVBU_PROJECT": "avbu",
            "DATAWORKS_EU_AVBU_ACCESSID": "test_id",
            "DATAWORKS_EU_AVBU_ACCESSKEY": "test_key",
            "DATAWORKS_EU_AVBU_ENDPOINT": "http://service.eu-central-1.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "dataworks_eu_avbu" in configs
            config = configs["dataworks_eu_avbu"]
            assert config["TYPE"] == "DATAWORKS"

    def test_parse_hologres_multi_instance(self):
        """Test parsing Hologres multi-instance configuration."""
        env_vars = {
            "HOLO_HK_CHATBI_TYPE": "HOLOGRES",
            "HOLO_HK_CHATBI_HOST": "hgpostcn-cn-11-cn-hongkong.hologres.aliyuncs.com",
            "HOLO_HK_CHATBI_USER": "BASIC$chatbi",
            "HOLO_HK_CHATBI_PASSWORD": "test_pass",
            "HOLO_HK_CHATBI_DBNAME": "chatbi",
            "HOLO_HK_CHATBI_PORT": "80",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "holo_hk_chatbi" in configs
            config = configs["holo_hk_chatbi"]
            assert config["TYPE"] == "HOLOGRES"
            assert config["HOST"] == "hgpostcn-cn-11-cn-hongkong.hologres.aliyuncs.com"

    def test_parse_mysql_multi_instance(self):
        """Test parsing MySQL multi-instance configuration."""
        env_vars = {
            "MYSQL_CN_ANTIGRAVITY_TYPE": "MySQL",
            "MYSQL_CN_ANTIGRAVITY_HOST": "111.rwlb.rds.aliyuncs.com",
            "MYSQL_CN_ANTIGRAVITY_USER": "bi_ro",
            "MYSQL_CN_ANTIGRAVITY_PASSWORD": "test_pass",
            "MYSQL_CN_ANTIGRAVITY_DB": "antigravity_prod",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "mysql_cn_antigravity" in configs
            config = configs["mysql_cn_antigravity"]
            assert config["TYPE"] == "MySQL"

    def test_parse_polardb_multi_instance(self):
        """Test parsing PolarDB multi-instance configuration."""
        env_vars = {
            "POLARDB_CN_INSTA360_TYPE": "POLARDB",
            "POLARDB_CN_INSTA360_HOST": "pc-111.rwlb.rds.aliyuncs.com",
            "POLARDB_CN_INSTA360_USER": "test_user",
            "POLARDB_CN_INSTA360_PASSWORD": "test_pass",
            "POLARDB_CN_INSTA360_DB": "insta360_data_collection",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "polardb_cn_insta360" in configs
            config = configs["polardb_cn_insta360"]
            assert config["TYPE"] == "POLARDB"

    def test_parse_redshift_multi_instance(self):
        """Test parsing Redshift multi-instance configuration."""
        env_vars = {
            "REDSHIFT_EU_AVBU_TYPE": "REDSHIFT",
            "REDSHIFT_EU_AVBU_HOST": "default-workgroup.111.eu-central-1.redshift-serverless.amazonaws.com",
            "REDSHIFT_EU_AVBU_PORT": "5439",
            "REDSHIFT_EU_AVBU_DB": "avbu",
            "REDSHIFT_EU_AVBU_USER": "admin",
            "REDSHIFT_EU_AVBU_PASSWORD": "test_pass",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            manager = ConnectionManager()
            configs = manager._parse_multi_instance_configs()
            
            assert "redshift_eu_avbu" in configs
            config = configs["redshift_eu_avbu"]
            assert config["TYPE"] == "REDSHIFT"

    def test_build_maxcompute_connection_string(self):
        """Test building MaxCompute connection string."""
        config = {
            "TYPE": "MAXCOMPUTE",
            "PROJECT": "bit_data_warehouse",
            "ACCESSID": "test_id",
            "ACCESSKEY": "test_key",
            "ENDPOINT": "http://service.cn-hongkong.maxcompute.aliyun.com/api",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("maxcompute_hk_bdw", config)
        
        assert conn_string is not None
        assert conn_string.startswith("maxcompute://")
        assert "test_id" in conn_string
        assert "test_key" in conn_string
        assert "bit_data_warehouse" in conn_string

    def test_build_dataworks_connection_string(self):
        """Test building DataWorks connection string (maps to MaxCompute)."""
        config = {
            "TYPE": "DATAWORKS",
            "PROJECT": "avbu",
            "ACCESSID": "test_id",
            "ACCESSKEY": "test_key",
            "ENDPOINT": "http://service.eu-central-1.maxcompute.aliyun.com/api",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("dataworks_eu_avbu", config)
        
        assert conn_string is not None
        assert conn_string.startswith("maxcompute://")

    def test_build_hologres_connection_string(self):
        """Test building Hologres connection string."""
        config = {
            "TYPE": "HOLOGRES",
            "HOST": "hgpostcn-cn-11-cn-hongkong.hologres.aliyuncs.com",
            "USER": "BASIC$chatbi",
            "PASSWORD": "test_pass",
            "DBNAME": "chatbi",
            "PORT": "80",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("holo_hk_chatbi", config)
        
        assert conn_string is not None
        assert conn_string.startswith("postgresql://")
        assert "chatbi" in conn_string

    def test_build_mysql_connection_string(self):
        """Test building MySQL connection string."""
        config = {
            "TYPE": "MySQL",
            "HOST": "111.rwlb.rds.aliyuncs.com",
            "USER": "bi_ro",
            "PASSWORD": "test_pass",
            "DB": "antigravity_prod",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("mysql_cn_antigravity", config)
        
        assert conn_string is not None
        assert conn_string.startswith("mysql+pymysql://")
        assert "antigravity_prod" in conn_string

    def test_build_polardb_connection_string(self):
        """Test building PolarDB connection string."""
        config = {
            "TYPE": "POLARDB",
            "HOST": "pc-111.rwlb.rds.aliyuncs.com",
            "USER": "test_user",
            "PASSWORD": "test_pass",
            "DB": "insta360_data_collection",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("polardb_cn_insta360", config)
        
        assert conn_string is not None
        assert conn_string.startswith("mysql+pymysql://")

    def test_build_redshift_connection_string(self):
        """Test building Redshift connection string."""
        config = {
            "TYPE": "REDSHIFT",
            "HOST": "default-workgroup.111.eu-central-1.redshift-serverless.amazonaws.com",
            "PORT": "5439",
            "DB": "avbu",
            "USER": "admin",
            "PASSWORD": "test_pass",
        }

        manager = ConnectionManager()
        conn_string = manager._build_connection_string("redshift_eu_avbu", config)
        
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
            "MAXCOMPUTE_HK_BDW_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_HK_BDW_PROJECT": "bit_data_warehouse",
            "MAXCOMPUTE_HK_BDW_ACCESSID": "test_id",
            "MAXCOMPUTE_HK_BDW_ACCESSKEY": "test_key",
            "MAXCOMPUTE_HK_BDW_ENDPOINT": "http://service.cn-hongkong.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                
                manager = ConnectionManager()
                platforms = manager.list_available_platforms()
                
                # Should have both legacy mysql and new maxcompute_hk_bdw
                assert "mysql" in platforms
                assert "maxcompute_hk_bdw" in platforms

    def test_incomplete_configuration_ignored(self):
        """Test that incomplete configurations are ignored."""
        env_vars = {
            # Missing ACCESSKEY
            "MAXCOMPUTE_HK_BDW_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_HK_BDW_PROJECT": "bit_data_warehouse",
            "MAXCOMPUTE_HK_BDW_ACCESSID": "test_id",
            "MAXCOMPUTE_HK_BDW_ENDPOINT": "http://service.cn-hongkong.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                
                manager = ConnectionManager()
                
                # Should not create engine for incomplete config
                # Check that build_connection_string returns None
                configs = manager._parse_multi_instance_configs()
                conn_string = manager._build_connection_string("maxcompute_hk_bdw", configs["maxcompute_hk_bdw"])
                assert conn_string is None

    def test_multiple_instances_same_platform(self):
        """Test multiple instances of the same platform type."""
        env_vars = {
            # First MaxCompute instance
            "MAXCOMPUTE_HK_BDW_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_HK_BDW_PROJECT": "bit_data_warehouse",
            "MAXCOMPUTE_HK_BDW_ACCESSID": "id1",
            "MAXCOMPUTE_HK_BDW_ACCESSKEY": "key1",
            "MAXCOMPUTE_HK_BDW_ENDPOINT": "http://service.cn-hongkong.maxcompute.aliyun.com/api",
            # Second MaxCompute instance
            "MAXCOMPUTE_EU_AVBU_TYPE": "MAXCOMPUTE",
            "MAXCOMPUTE_EU_AVBU_PROJECT": "avbu",
            "MAXCOMPUTE_EU_AVBU_ACCESSID": "id2",
            "MAXCOMPUTE_EU_AVBU_ACCESSKEY": "key2",
            "MAXCOMPUTE_EU_AVBU_ENDPOINT": "http://service.eu-central-1.maxcompute.aliyun.com/api",
        }

        with patch.dict("os.environ", env_vars, clear=True):
            with patch('src.dw_mcp.connections.create_engine') as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                
                manager = ConnectionManager()
                platforms = manager.list_available_platforms()
                
                # Should have both instances
                assert "maxcompute_hk_bdw" in platforms
                assert "maxcompute_eu_avbu" in platforms
