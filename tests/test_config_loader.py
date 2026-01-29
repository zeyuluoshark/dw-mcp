"""Tests for configuration loader."""

import os
import pytest
from pathlib import Path
from src.dw_mcp.config_loader import load_env_file


def test_load_env_file_basic(tmp_path):
    """Test loading a basic .env file."""
    # Create a temporary .env file
    env_file = tmp_path / '.env'
    env_file.write_text('''
# Comment line
TEST_VAR1=value1
TEST_VAR2="value2"
TEST_VAR3='value3'

TEST_VAR4=value with spaces
''')
    
    # Clear any existing env vars
    for key in ['TEST_VAR1', 'TEST_VAR2', 'TEST_VAR3', 'TEST_VAR4']:
        if key in os.environ:
            del os.environ[key]
    
    # Load the file
    result = load_env_file(str(env_file))
    
    assert result is True
    assert os.environ.get('TEST_VAR1') == 'value1'
    assert os.environ.get('TEST_VAR2') == 'value2'
    assert os.environ.get('TEST_VAR3') == 'value3'
    assert os.environ.get('TEST_VAR4') == 'value with spaces'
    
    # Cleanup
    for key in ['TEST_VAR1', 'TEST_VAR2', 'TEST_VAR3', 'TEST_VAR4']:
        if key in os.environ:
            del os.environ[key]


def test_load_env_file_not_found():
    """Test loading a non-existent .env file."""
    result = load_env_file('/nonexistent/path/.env')
    assert result is False


def test_load_env_file_env_vars_take_precedence(tmp_path):
    """Test that existing env vars take precedence over .env file."""
    # Create a temporary .env file
    env_file = tmp_path / '.env'
    env_file.write_text('TEST_PRECEDENCE=from_file')
    
    # Set an env var
    os.environ['TEST_PRECEDENCE'] = 'from_env'
    
    # Load the file
    result = load_env_file(str(env_file))
    
    assert result is True
    # Should keep the env var value, not the file value
    assert os.environ.get('TEST_PRECEDENCE') == 'from_env'
    
    # Cleanup
    del os.environ['TEST_PRECEDENCE']


def test_load_env_file_empty_lines_and_comments(tmp_path):
    """Test that empty lines and comments are handled correctly."""
    env_file = tmp_path / '.env'
    env_file.write_text('''
# This is a comment
# Another comment

TEST_VALID=valid_value

# Comment in between
''')
    
    if 'TEST_VALID' in os.environ:
        del os.environ['TEST_VALID']
    
    result = load_env_file(str(env_file))
    
    assert result is True
    assert os.environ.get('TEST_VALID') == 'valid_value'
    
    # Cleanup
    if 'TEST_VALID' in os.environ:
        del os.environ['TEST_VALID']


def test_load_env_file_equals_in_value(tmp_path):
    """Test handling values with = character."""
    env_file = tmp_path / '.env'
    env_file.write_text('TEST_EQUALS=value=with=equals')
    
    if 'TEST_EQUALS' in os.environ:
        del os.environ['TEST_EQUALS']
    
    result = load_env_file(str(env_file))
    
    assert result is True
    assert os.environ.get('TEST_EQUALS') == 'value=with=equals'
    
    # Cleanup
    if 'TEST_EQUALS' in os.environ:
        del os.environ['TEST_EQUALS']


def test_load_env_file_auto_discover(tmp_path, monkeypatch):
    """Test auto-discovery of .env file in current directory."""
    # Change to tmp directory
    monkeypatch.chdir(tmp_path)
    
    # Create .env file
    env_file = tmp_path / '.env'
    env_file.write_text('TEST_AUTO=auto_discovered')
    
    if 'TEST_AUTO' in os.environ:
        del os.environ['TEST_AUTO']
    
    # Load without specifying path
    result = load_env_file()
    
    assert result is True
    assert os.environ.get('TEST_AUTO') == 'auto_discovered'
    
    # Cleanup
    if 'TEST_AUTO' in os.environ:
        del os.environ['TEST_AUTO']
