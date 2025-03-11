


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
                    if query.strip().lower().startswith("delete"):
                        # Special handling for deleting employees (manager dependency)
                        employee_id = user_inputs.get("employee_id")

                        if employee_id:
                            # Check for dependent employees
                            check_query = text("SELECT COUNT(*) FROM employees WHERE manager_id = :employee_id;")
                            dependent_count = session.execute(check_query, {"employee_id": employee_id}).scalar()

                            if dependent_count > 0:
                                # Update dependent employees to remove manager reference before deletion
                                update_query = text("UPDATE employees SET manager_id = NULL WHERE manager_id = :employee_id;")
                                session.execute(update_query, {"employee_id": employee_id})
                                session.commit()

                            # Proceed with employee deletion
                            delete_query = text(query)
                            session.execute(delete_query, user_inputs)
                            session.commit()

                            results.append({
                                "use_case": query_info["use_case"],
                                "query": query,
                                "results": f"Employee ID {employee_id} deleted successfully after resolving dependencies.",
                                "user_input_columns": query_info.get("user_input_columns", [])
                            })
                        else:
                            session.execute(text(query), user_inputs)
                            session.commit()
                            results.append({
                                "use_case": query_info["use_case"],
                                "query": query,
                                "results": "Record deleted successfully.",
                                "user_input_columns": query_info.get("user_input_columns", [])
                            })

                    else:
                        # For INSERT and UPDATE queries, commit after execution
                        result = session.execute(text(query), user_inputs)
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
