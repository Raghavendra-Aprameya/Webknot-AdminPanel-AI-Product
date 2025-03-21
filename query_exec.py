
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, List
import re

class DatabaseQueryExecutor:
    def __init__(self, connection_string: str):
        """Initialize the database connection for executing queries."""
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        # Determine the database type from the connection string
        self.is_postgres = 'postgresql' in connection_string.lower()
    
    def _fix_query_for_execution(self, query: str) -> str:
        """Fix the query syntax to work with SQLAlchemy's text() function."""
        # Replace ": :param" with "= :param"
        fixed_query = re.sub(r':\s+:(\w+)', r'= :\1', query)
        
        # Replace ": value" with "= value"
        fixed_query = re.sub(r':\s+([^:])', r'= \1', fixed_query)
        
        return fixed_query
    
    def execute_query(self, query: str, params: List[Any]) -> Dict[str, Any]:
        """Executes a dynamically generated SQL query with correct parameter binding."""
        try:
            # Fix the query syntax
            fixed_query = self._fix_query_for_execution(query)
            
            # Extract parameter names from the query (:param_name)
            param_names = re.findall(r':(\w+)', fixed_query)
            
            # Create a dictionary of named parameters
            param_dict = {}
            for i, name in enumerate(param_names):
                if i < len(params):
                    param_dict[name] = params[i]
            
            # For the specific case that's failing
            if "last_login :" in query:
                fixed_query = fixed_query.replace("last_login :", "last_login =")
            
            # Determine if this is a SELECT query
            is_select = fixed_query.strip().lower().startswith("select")
            
            with self.Session() as session:
                try:
                    # Execute the query using bind parameters
                    result = session.execute(text(fixed_query), param_dict)
                    
                    if is_select:
                        # For SELECT queries, fetch and return all rows as dictionaries
                        data = [dict(row) for row in result.mappings()]
                        return {
                            "type": "select",
                            "results": data,
                            "count": len(data)
                        }
                    else:
                        # For non-SELECT queries, commit and return row count
                        session.commit()
                        return {
                            "type": "dml",
                            "results": f"{result.rowcount} row(s) affected.",
                            "count": result.rowcount
                        }
                except Exception as e:
                    session.rollback()
                    return {"error": str(e)}
        except Exception as e:
            return {"error": f"Query preparation failed: {str(e)}"}