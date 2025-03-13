
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, List

class DatabaseQueryExecutor:
    def __init__(self, connection_string: str):
        """Initialize the database connection for executing queries."""
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def execute_query(self, query: str, user_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a dynamically generated SQL query.

        :param query: The generated SQL query.
        :param user_inputs: Dictionary of user-provided parameters.
        :return: Query execution results.
        """
        with self.Session() as session:
            try:
                if query.strip().lower().startswith("select"):
                    result = session.execute(text(query), user_inputs)
                    return {"results": [dict(row) for row in result.mappings()] or "No records found."}

                session.execute(text(query), user_inputs)
                session.commit()
                return {"results": "Query executed successfully."}

            except Exception as e:
                session.rollback()
                return {"error": str(e)}
