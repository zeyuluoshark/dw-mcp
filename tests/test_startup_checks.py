"""Tests for startup checks and configuration auto-fix."""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from src.dw_mcp.startup_checks import (
    check_python_version,
    check_dependencies,
    auto_fix_config,
    find_python310,
)


def test_check_python_version_pass(capsys):
    """Test that check_python_version passes for Python 3.10+."""
    # This test assumes we're running on Python 3.10+
    if sys.version_info >= (3, 10):
        result = check_python_version()
        assert result is True
        
        captured = capsys.readouterr()
        assert "✓ Python version:" in captured.out


def test_check_python_version_fail():
    """Test that check_python_version fails for Python < 3.10."""
    # Create a named tuple to properly mock sys.version_info
    from collections import namedtuple
    VersionInfo = namedtuple('VersionInfo', ['major', 'minor', 'micro', 'releaselevel', 'serial'])
    
    with patch('sys.version_info', VersionInfo(3, 9, 0, 'final', 0)):
        result = check_python_version()
        assert result is False


def test_auto_fix_dataworks_endpoint():
    """Test auto-fixing DATAWORKS ENDPOINT generation."""
    # Set up test environment
    test_env = {
        'DATAWORKS_HK_BDW_REGION': 'cn-hongkong',
        'DATAWORKS_HK_BDW_TYPE': 'DATAWORKS',
    }
    
    with patch.dict(os.environ, test_env, clear=False):
        fixes = auto_fix_config()
        
        # Should have generated ENDPOINT
        assert 'DATAWORKS_HK_BDW_ENDPOINT' in os.environ
        assert 'cn-hongkong' in os.environ['DATAWORKS_HK_BDW_ENDPOINT']
        assert any('Generated DATAWORKS_HK_BDW_ENDPOINT' in fix for fix in fixes)
        
        # Cleanup
        if 'DATAWORKS_HK_BDW_ENDPOINT' in os.environ:
            del os.environ['DATAWORKS_HK_BDW_ENDPOINT']


def test_auto_fix_hologres_type():
    """Test auto-fixing missing HOLOGRES TYPE."""
    test_env = {
        'HOLO_HK_CHATBI_HOST': 'test-instance.hologres.aliyuncs.com',
    }
    
    with patch.dict(os.environ, test_env, clear=False):
        fixes = auto_fix_config()
        
        # Should have added TYPE
        assert 'HOLO_HK_CHATBI_TYPE' in os.environ
        assert os.environ['HOLO_HK_CHATBI_TYPE'] == 'HOLOGRES'
        assert any('Added HOLO_HK_CHATBI_TYPE' in fix for fix in fixes)
        
        # Cleanup
        if 'HOLO_HK_CHATBI_TYPE' in os.environ:
            del os.environ['HOLO_HK_CHATBI_TYPE']


def test_auto_fix_redshift_type_case():
    """Test auto-fixing REDSHIFT TYPE case."""
    test_env = {
        'REDSHIFT_EU_AVBU_TYPE': 'redshift',  # lowercase
    }
    
    with patch.dict(os.environ, test_env, clear=False):
        fixes = auto_fix_config()
        
        # Should have fixed case
        assert os.environ['REDSHIFT_EU_AVBU_TYPE'] == 'REDSHIFT'
        assert any('Fixed REDSHIFT_EU_AVBU_TYPE' in fix for fix in fixes)


def test_auto_fix_mysql_type_case():
    """Test auto-fixing MYSQL TYPE case."""
    test_env = {
        'MYSQL_CN_TEST_TYPE': 'mysql',  # lowercase
    }
    
    with patch.dict(os.environ, test_env, clear=False):
        fixes = auto_fix_config()
        
        # Should have fixed case
        assert os.environ['MYSQL_CN_TEST_TYPE'] == 'MYSQL'
        assert any('Fixed MYSQL_CN_TEST_TYPE' in fix for fix in fixes)


def test_auto_fix_polardb_type_case():
    """Test auto-fixing POLARDB TYPE case."""
    test_env = {
        'POLARDB_CN_TEST_TYPE': 'polardb',  # lowercase
    }
    
    with patch.dict(os.environ, test_env, clear=False):
        fixes = auto_fix_config()
        
        # Should have fixed case
        assert os.environ['POLARDB_CN_TEST_TYPE'] == 'POLARDB'
        assert any('Fixed POLARDB_CN_TEST_TYPE' in fix for fix in fixes)


def test_auto_fix_no_issues():
    """Test auto_fix_config when there are no issues."""
    # Clear any test env vars
    fixes = auto_fix_config()
    
    # May or may not have fixes depending on actual environment
    # Just ensure it doesn't crash
    assert isinstance(fixes, list)


def test_find_python310():
    """Test finding Python 3.10+."""
    python_cmd = find_python310()
    
    # Should find the current Python since we're running on 3.10+
    assert python_cmd is not None
    assert isinstance(python_cmd, str)


@patch('builtins.__import__')
def test_check_dependencies_all_installed(mock_import, capsys):
    """Test check_dependencies when all deps are installed."""
    # Mock successful imports
    mock_import.return_value = MagicMock()
    
    result = check_dependencies()
    
    # Should pass
    assert result is True
    
    captured = capsys.readouterr()
    assert "✓ All dependencies installed" in captured.out


@patch('builtins.__import__')
def test_check_dependencies_missing(mock_import, capsys):
    """Test check_dependencies when some deps are missing."""
    # Mock ImportError for specific module
    def import_side_effect(name, *args, **kwargs):
        if name == 'odps':
            raise ImportError(f"No module named '{name}'")
        return MagicMock()
    
    mock_import.side_effect = import_side_effect
    
    result = check_dependencies()
    
    # Should fail
    assert result is False
    
    captured = capsys.readouterr()
    assert "❌ Missing dependencies:" in captured.out
    assert "pyodps" in captured.out
