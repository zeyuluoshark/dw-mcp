"""Configuration loader for DW MCP server."""

import os
from pathlib import Path
from typing import Optional


def load_env_file(env_file: Optional[str] = None) -> bool:
    """
    Load environment variables from a .env file.
    
    Args:
        env_file: Path to .env file. If None, looks for .env in current directory
        
    Returns:
        True if file was loaded, False otherwise
    """
    if env_file is None:
        # Look for .env in current directory and parent directories
        current_dir = Path.cwd()
        for _ in range(3):  # Check up to 3 levels up
            env_path = current_dir / '.env'
            if env_path.exists():
                env_file = str(env_path)
                break
            current_dir = current_dir.parent
    
    if env_file is None:
        return False
    
    env_path = Path(env_file)
    if not env_path.exists():
        return False
    
    # Parse .env file
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Parse KEY=VALUE format
                if '=' in line:
                    # Split only on first =
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Only set if not already in environment (env vars take precedence)
                    if key not in os.environ:
                        os.environ[key] = value
        
        return True
    except Exception as e:
        print(f"⚠️  Warning: Failed to load .env file: {e}")
        return False
