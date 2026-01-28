"""SQL safety checker to prevent destructive operations."""

import re
from typing import Tuple, List


class SQLSafetyChecker:
    """Checks SQL queries for destructive operations."""

    # Destructive SQL keywords/patterns
    DESTRUCTIVE_KEYWORDS = [
        r'\bDROP\s+TABLE\b',
        r'\bDROP\s+DATABASE\b',
        r'\bDROP\s+SCHEMA\b',
        r'\bTRUNCATE\b',
        r'\bDELETE\s+FROM\b',
        r'\bUPDATE\s+\w+\s+SET\b',
        r'\bINSERT\s+INTO\b',
        r'\bCREATE\s+TABLE\b',
        r'\bCREATE\s+DATABASE\b',
        r'\bCREATE\s+SCHEMA\b',
        r'\bALTER\s+TABLE\b',
        r'\bMERGE\s+INTO\b',
    ]

    @staticmethod
    def is_destructive(query: str) -> Tuple[bool, List[str]]:
        """
        Check if a query contains destructive operations.
        
        Args:
            query: SQL query to check
            
        Returns:
            Tuple of (is_destructive, list of matched patterns)
        """
        query_upper = query.upper()
        matched_patterns = []
        
        for pattern in SQLSafetyChecker.DESTRUCTIVE_KEYWORDS:
            if re.search(pattern, query_upper, re.IGNORECASE):
                # Extract the matched keyword for reporting
                match = re.search(pattern, query_upper, re.IGNORECASE)
                if match:
                    matched_patterns.append(match.group(0))
        
        return len(matched_patterns) > 0, matched_patterns

    @staticmethod
    def is_select_query(query: str) -> bool:
        """
        Check if query is a SELECT statement.
        
        Args:
            query: SQL query to check
            
        Returns:
            True if query is a SELECT statement
        """
        query_stripped = query.strip().upper()
        # Check for SELECT, WITH (CTE), or SHOW statements
        return (
            query_stripped.startswith('SELECT') or
            query_stripped.startswith('WITH') or
            query_stripped.startswith('SHOW') or
            query_stripped.startswith('DESCRIBE') or
            query_stripped.startswith('DESC') or
            query_stripped.startswith('EXPLAIN')
        )

    @staticmethod
    def suggest_limit(query: str, default_limit: int = 100) -> str:
        """
        Add LIMIT clause to SELECT queries if not present.
        
        Args:
            query: SQL query
            default_limit: Default limit to apply
            
        Returns:
            Query with LIMIT clause
        """
        query_upper = query.strip().upper()
        
        # Check if query already has LIMIT
        if 'LIMIT' in query_upper:
            return query
        
        # Only add LIMIT to SELECT queries
        if not SQLSafetyChecker.is_select_query(query):
            return query
        
        # Add LIMIT clause
        query_clean = query.strip().rstrip(';')
        return f"{query_clean} LIMIT {default_limit}"

    @staticmethod
    def validate_query(
        query: str,
        allow_destructive: bool = False,
        auto_limit: bool = True,
        default_limit: int = 100
    ) -> Tuple[bool, str, str]:
        """
        Validate a SQL query for safety.
        
        Args:
            query: SQL query to validate
            allow_destructive: Whether to allow destructive operations
            auto_limit: Whether to automatically add LIMIT to SELECT queries
            default_limit: Default limit value
            
        Returns:
            Tuple of (is_valid, processed_query, message)
        """
        if not query or not query.strip():
            return False, query, "Empty query"
        
        # Check for destructive operations
        is_destructive, patterns = SQLSafetyChecker.is_destructive(query)
        
        if is_destructive and not allow_destructive:
            return False, query, (
                f"Destructive operation detected: {', '.join(patterns)}. "
                "This is a read-only assistant. Please confirm if you really want to execute this."
            )
        
        # Add LIMIT if applicable
        processed_query = query
        if auto_limit and SQLSafetyChecker.is_select_query(query):
            processed_query = SQLSafetyChecker.suggest_limit(query, default_limit)
        
        return True, processed_query, "Query validated successfully"
