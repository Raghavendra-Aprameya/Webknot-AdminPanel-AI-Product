from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import re

class DatabaseQueryExecutor:
    def __init__(self, connection_string: str):
        """
        Initialize database connection for query execution
        
        :param connection_string: SQLAlchemy database connection string
        """
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def extract_affected_columns(self, query: str) -> List[str]:
        """
        Extract column names from SQL query where user input might be required.

        :param query: SQL query string
        :return: List of column names affected by the query, prefixed with table names
        """
        column_patterns = [
            r"WHERE\s+([\w\.\s,=><'\"%()-]+)",  
            r"SET\s+([\w\.\s,=><'\"%()-]+)",    
            r"INSERT\s+INTO\s+\w+\s*\((.*?)\)",  
            r"VALUES\s*\((.*?)\)"               
        ]

        affected_columns = set()
        for pattern in column_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            for match in matches:
                columns = re.split(r",\s*", match)  
                affected_columns.update(col.split("=")[0].strip() for col in columns)

        # Ensure table name prefixing
        affected_columns_prefixed = [col if "." in col else f"unknown_table.{col}" for col in affected_columns]

        return list(affected_columns_prefixed)

    def execute_queries(self, queries: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Execute a list of SQL queries and return results along with affected columns.

        :param queries: List of query dictionaries containing SQL and metadata
        :return: List of query execution results, including extracted column names
        """
        results = []
        with self.Session() as session:
            for query_info in queries:
                query = query_info['query']
                print(f"Executing Query:\n{query}")

                affected_columns = self.extract_affected_columns(query)

                try:
                    result = session.execute(text(query))
                    query_results = [dict(row) for row in result.mappings()]

                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "results": query_results,
                        "affected_columns": affected_columns  
                    })

                except Exception as e:
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": str(e),
                        "affected_columns": affected_columns  
                    })

        return results
