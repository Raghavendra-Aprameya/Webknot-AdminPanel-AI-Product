from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

class DatabaseQueryExecutor:
    def __init__(self, connection_string: str):
        """Initialize database connection for query execution."""
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def execute_queries(self, queries: List[Dict[str, str]], user_inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute SQL queries dynamically with user input values.

        :param queries: List of SQL query dictionaries with placeholders.
        :param user_inputs: Dictionary containing user-entered values for query parameters.
        :return: Query execution results.
        """
        results = []
        with self.Session() as session:
            for query_info in queries:
                query = query_info["query"]

                try:
                    # ✅ Handle SELECT queries and fetch results
                    if query.strip().lower().startswith("select"):
                        result = session.execute(text(query), user_inputs)
                        query_results = [dict(row) for row in result.mappings()]  # Convert to list of dictionaries

                        results.append({
                            "use_case": query_info["use_case"],
                            "query": query,
                            "results": query_results if query_results else "No records found.",
                            "user_input_columns": query_info.get("user_input_columns", [])
                        })

                    elif query.strip().lower().startswith("delete"):
                        # ✅ Handle DELETE queries
                        session.execute(text(query), user_inputs)
                        session.commit()
                        results.append({
                            "use_case": query_info["use_case"],
                            "query": query,
                            "results": "Record deleted successfully.",
                            "user_input_columns": query_info.get("user_input_columns", [])
                        })

                    else:
                        # ✅ Handle INSERT and UPDATE queries
                        session.execute(text(query), user_inputs)
                        session.commit()
                        results.append({
                            "use_case": query_info["use_case"],
                            "query": query,
                            "results": "Query executed successfully.",
                            "user_input_columns": query_info.get("user_input_columns", [])
                        })

                except IntegrityError:
                    session.rollback()
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": "Foreign key constraint error: Cannot delete or update this record as it is referenced elsewhere.",
                        "user_input_columns": query_info.get("user_input_columns", [])
                    })
                except Exception as e:
                    session.rollback()
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": str(e),
                        "user_input_columns": query_info.get("user_input_columns", [])
                    })

        return results
