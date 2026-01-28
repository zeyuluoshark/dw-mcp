"""Setup script for dw-mcp package."""

from setuptools import setup, find_packages

setup(
    name="dw-mcp",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "mcp>=0.9.0",
        "sqlalchemy>=2.0.0",
        "pymysql>=1.1.0",
        "psycopg2-binary>=2.9.0",
        "redshift-connector>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
        ],
    },
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "dw-mcp=dw_mcp.server:main",
        ],
    },
)
