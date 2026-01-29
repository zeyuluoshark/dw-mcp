"""Startup checks and configuration auto-fix for DW MCP server."""

import os
import sys
import re
import shutil
import subprocess
from typing import Optional, List, Tuple


def find_python310() -> Optional[str]:
    """
    Find a Python 3.10+ installation.
    
    Returns:
        Path to Python 3.10+ executable, or None if not found
    """
    # Check if current Python is already 3.10+
    if sys.version_info >= (3, 10):
        return sys.executable
    
    # Try to find other Python versions
    for version in ['3.12', '3.11', '3.10']:
        python_cmd = f'python{version}'
        if shutil.which(python_cmd):
            try:
                result = subprocess.run(
                    [python_cmd, '--version'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    # Parse version
                    version_str = result.stdout.strip()
                    match = re.search(r'(\d+)\.(\d+)', version_str)
                    if match:
                        major, minor = int(match.group(1)), int(match.group(2))
                        if major == 3 and minor >= 10:
                            return python_cmd
            except Exception:
                continue
    
    return None


def check_python_version() -> bool:
    """
    Check if Python version is 3.10 or higher.
    
    Returns:
        True if version is adequate, False otherwise
    """
    current_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    
    if sys.version_info < (3, 10):
        print("❌ Python version too low")
        print(f"   Current: {current_version}")
        print(f"   Required: >=3.10")
        print("\n解决方案 (Solutions):")
        
        # Try to find a suitable Python version
        python_cmd = find_python310()
        if python_cmd and python_cmd != sys.executable:
            print(f"1. Use existing Python 3.10+:")
            print(f"   {python_cmd} -m src.dw_mcp.server")
        else:
            print("1. Install Python 3.10+:")
            print("   macOS: brew install python@3.10")
            print("   Ubuntu: sudo apt install python3.10")
            print("   Windows: Download from python.org")
            print("\n2. Or use pyenv:")
            print("   pyenv install 3.10.0")
            print("   pyenv local 3.10.0")
        
        return False
    
    print(f"✓ Python version: {current_version}")
    return True


def check_dependencies() -> bool:
    """
    Check if all required dependencies are installed.
    
    Returns:
        True if all dependencies are installed, False otherwise
    """
    REQUIRED_DEPS = {
        'mcp': 'mcp>=0.9.0',
        'sqlalchemy': 'sqlalchemy>=2.0.0',
        'pymysql': 'pymysql>=1.1.0',
        'psycopg2': 'psycopg2-binary>=2.9.0',
        'redshift_connector': 'redshift-connector>=2.0.0',
        'sqlalchemy_redshift': 'sqlalchemy-redshift>=0.8.0',
        'odps': 'pyodps>=0.11.0',
    }
    
    missing = []
    for module, package in REQUIRED_DEPS.items():
        try:
            __import__(module)
        except ImportError:
            missing.append(package)
    
    if missing:
        print("❌ Missing dependencies:")
        for pkg in missing:
            print(f"   - {pkg}")
        print("\n解决方案 (Solutions):")
        print("1. Install missing dependencies:")
        print(f"   pip install {' '.join(missing)}")
        print("\n2. Or install all dependencies:")
        print("   pip install -r requirements.txt")
        return False
    
    print("✓ All dependencies installed")
    return True


def auto_fix_config() -> List[str]:
    """
    Auto-fix common configuration issues.
    
    Returns:
        List of fixes applied
    """
    fixes = []
    
    # Fix 1: Generate DATAWORKS ENDPOINT from REGION
    for key in list(os.environ.keys()):
        if key.startswith('DATAWORKS_') and key.endswith('_REGION'):
            # Extract instance identifier
            parts = key.split('_')
            if len(parts) >= 4:
                # DATAWORKS_HK_BDW_REGION -> DATAWORKS_HK_BDW
                instance_prefix = '_'.join(parts[:-1])
                endpoint_key = f'{instance_prefix}_ENDPOINT'
                
                if endpoint_key not in os.environ:
                    region = os.environ[key]
                    # Generate endpoint based on region
                    endpoint = f'http://service.{region}.maxcompute.aliyun.com/api'
                    os.environ[endpoint_key] = endpoint
                    fixes.append(f'Generated {endpoint_key}')
    
    # Fix 2: Add missing HOLOGRES TYPE
    for key in list(os.environ.keys()):
        if (key.startswith('HOLO_') or key.startswith('HOLOGRES_')) and key.endswith('_HOST'):
            # HOLO_HK_CHATBI_HOST -> HOLO_HK_CHATBI_TYPE
            type_key = key.replace('_HOST', '_TYPE')
            if type_key not in os.environ:
                os.environ[type_key] = 'HOLOGRES'
                fixes.append(f'Added {type_key}=HOLOGRES')
    
    # Fix 3: Fix REDSHIFT TYPE case (lowercase to uppercase)
    for key in list(os.environ.keys()):
        if key.startswith('REDSHIFT_') and key.endswith('_TYPE'):
            value = os.environ[key]
            if value and value.lower() == 'redshift' and value != 'REDSHIFT':
                os.environ[key] = 'REDSHIFT'
                fixes.append(f'Fixed {key} (lowercase → REDSHIFT)')
    
    # Fix 4: Fix MYSQL TYPE case
    for key in list(os.environ.keys()):
        if key.startswith('MYSQL_') and key.endswith('_TYPE'):
            value = os.environ[key]
            if value and value.lower() == 'mysql' and value != 'MYSQL':
                os.environ[key] = 'MYSQL'
                fixes.append(f'Fixed {key} (lowercase → MYSQL)')
    
    # Fix 5: Fix POLARDB TYPE case
    for key in list(os.environ.keys()):
        if key.startswith('POLARDB_') and key.endswith('_TYPE'):
            value = os.environ[key]
            if value and value.lower() == 'polardb' and value != 'POLARDB':
                os.environ[key] = 'POLARDB'
                fixes.append(f'Fixed {key} (lowercase → POLARDB)')
    
    return fixes


def print_startup_banner(conn_manager) -> None:
    """
    Print a friendly startup banner with configuration info.
    
    Args:
        conn_manager: ConnectionManager instance
    """
    platforms = conn_manager.list_available_platforms()
    
    print("=" * 70)
    print("MCP Server Started Successfully! 🚀")
    print("=" * 70)
    
    if not platforms:
        print("\n⚠️  No platforms configured")
        print("\nTo configure platforms, set environment variables:")
        print("  - MAXCOMPUTE_* or DATAWORKS_*")
        print("  - HOLOGRES_* or HOLO_*")
        print("  - MYSQL_*")
        print("  - POLARDB_*")
        print("  - REDSHIFT_*")
        print("\nSee README.md for configuration examples.")
    else:
        print(f"\nConfigured Platform Instances ({len(platforms)}):\n")
        
        # Group by platform type
        by_type = {}
        for platform in platforms:
            # Extract platform type from instance key
            platform_type = platform.split('_')[0].upper()
            if platform_type not in by_type:
                by_type[platform_type] = []
            by_type[platform_type].append(platform)
        
        # Print grouped platforms
        for platform_type in sorted(by_type.keys()):
            print(f"  {platform_type}:")
            for platform in sorted(by_type[platform_type]):
                # Try to get engine to check status
                engine = conn_manager.get_engine(platform)
                status = "✓" if engine else "✗"
                print(f"    {status} {platform}")
            print()
    
    print("Server is waiting for MCP client connections...")
    print("=" * 70)
    print()


def run_startup_checks(conn_manager=None) -> bool:
    """
    Run all startup checks and configuration fixes.
    
    Args:
        conn_manager: Optional ConnectionManager instance for status display
    
    Returns:
        True if all critical checks pass, False otherwise
    """
    print("Running startup checks...\n")
    
    # Check 1: Python version
    if not check_python_version():
        return False
    
    # Check 2: Dependencies
    if not check_dependencies():
        print("\n⚠️  Please install missing dependencies before continuing.")
        return False
    
    # Check 3: Auto-fix configuration
    print("\nChecking configuration...")
    fixes = auto_fix_config()
    
    if fixes:
        print("⚠️  Configuration issues detected and auto-fixed:")
        for fix in fixes:
            print(f"   ✓ {fix}")
    else:
        print("✓ Configuration looks good")
    
    print()
    
    # Print startup banner if connection manager provided
    if conn_manager:
        print_startup_banner(conn_manager)
    
    return True
