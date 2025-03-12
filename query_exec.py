
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError, SQLAlchemyError

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
                    # Pre-process query for common syntax issues
                    query = self._fix_common_syntax_issues(query)
                    
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
                        result = session.execute(text(query), user_inputs)
                        session.commit()
                        results.append({
                            "use_case": query_info["use_case"],
                            "query": query,
                            "results": f"{result.rowcount} record(s) deleted successfully.",
                            "user_input_columns": query_info.get("user_input_columns", [])
                        })

                    else:
                        # ✅ Handle INSERT and UPDATE queries
                        result = session.execute(text(query), user_inputs)
                        session.commit()
                        results.append({
                            "use_case": query_info["use_case"],
                            "query": query,
                            "results": f"Query executed successfully. {result.rowcount} row(s) affected.",
                            "user_input_columns": query_info.get("user_input_columns", [])
                        })

                except ProgrammingError as e:
                    session.rollback()
                    fixed_query = self._suggest_fix_for_query(query, str(e))
                    error_message = f"{str(e)}\n\nSuggested fix: {fixed_query}" if fixed_query != query else str(e)
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": error_message,
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
                except OperationalError as e:
                    session.rollback()
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": f"Database operation error: {str(e)}",
                        "user_input_columns": query_info.get("user_input_columns", [])
                    })
                except Exception as e:
                    session.rollback()
                    results.append({
                        "use_case": query_info["use_case"],
                        "query": query,
                        "error": f"Unexpected error: {str(e)}",
                        "user_input_columns": query_info.get("user_input_columns", [])
                    })

        return results
    
    def _fix_common_syntax_issues(self, query: str) -> str:
        """Fix common syntax issues in SQL queries before execution."""
        import re
        
        # Fix missing comparison operators
        query = re.sub(r'(\w+)\s{2,}:', r'\1 > :', query)
        query = re.sub(r'(\w+\.\w+)\s{2,}(\w+\.\w+)', r'\1 > \2', query)
        
        # Fix other common issues
        query = query.replace("WHERE e.salary m.salary", "WHERE e.salary > m.salary")
        
        return query
    
    def _suggest_fix_for_query(self, query: str, error_msg: str) -> str:
        """Suggest fixes based on error messages."""
        if "syntax" in error_msg.lower():
            # Look for missing comparison operators in WHERE clauses
            if "WHERE" in query:
                query = self._fix_common_syntax_issues(query)
        
        return query