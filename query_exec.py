from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import re

class DatabaseQueryExecutor:
    def __init__(self, connection_string: str):
        """Initialize database connection for query execution."""
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def extract_user_input_columns(self, query: str) -> List[str]:
        """
        Extract columns that require user input from an SQL query.

        :param query: SQL query string
        :return: List of columns requiring user input.
        """
        user_input_patterns = [
            r"WHERE\s+([\w\.\s,=><'\"%()-]+)",  # Capture conditions in WHERE
            r"SET\s+([\w\.\s,=><'\"%()-]+)",    # Capture columns in UPDATE SET
            r"VALUES\s*\((.*?)\)"               # Capture values in INSERT queries
        ]

        user_input_columns = set()

        for pattern in user_input_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):  
                    match = match[0]
                columns = re.split(r",\s*", match)
                for col in columns:
                    col = col.split("=")[0].strip()
                    if "." in col:
                        user_input_columns.add(col)
                    else:
                        user_input_columns.add(f"unknown_table.{col}")

        return list(user_input_columns)

    def execute_queries(self, queries: List[Dict[str, str]], user_inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute SQL queries dynamically with user input values.

        :param queries: List of SQL query dictionaries with placeholders.
        :param user_inputs: Dictionary containing user-entered values for query parameters.
        :return: Query execution results with only user input columns.
        """
        results = []
        with self.Session() as session:
            for query_info in queries:
                query = query_info["query"]
                user_input_columns = self.extract_user_input_columns(query)

                # Secure Parameterized Query Execution
                try:
                    query_params = {}
                    for col in user_input_columns:
                        col_name = col.split(".")[-1]  # Extract column name without table prefix
                        if col_name in user_inputs:
                            query_params[col_name] = user_inputs[col_name]

                    result = session.execute(text(query), query_params)
                    query_results = [dict(row) for row in result.mappings()]

                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "results": query_results,
                        "user_input_columns": user_input_columns  
                    })

                except Exception as e:
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": str(e),
                        "user_input_columns": user_input_columns  
                    })

        return results
