
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
import mysql.connector
from config import CONNECTION_STRING, GOOGLE_API_KEY, LLM_MODEL, DB_TYPE
from db_extract import DatabaseSchemaExtractor
from query_generator import FinanceQueryGenerator
from query_exec import DatabaseQueryExecutor

load_dotenv()

# Load database credentials from .env file
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

app = FastAPI()

# Cache for storing generated use cases to avoid regenerating them for each request
use_case_cache = None

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def execute_query(query: str, params: Optional[List[Any]] = None):
    """Executes a given SQL query and returns affected rows."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        return affected_rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

def get_all_use_cases():
    """Fetches and caches all use cases from the database schema."""
    global use_case_cache
    
    if use_case_cache is None:
        try:
            schema_extractor = DatabaseSchemaExtractor(CONNECTION_STRING)
            schema = schema_extractor.get_schema()
            
            query_generator = FinanceQueryGenerator(
                schema=schema,
                api_key=GOOGLE_API_KEY,
                db_type=DB_TYPE,
                model=LLM_MODEL
            )
            
            use_case_cache = query_generator.generate_use_cases()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to generate use cases: {str(e)}")
    
    return use_case_cache

@app.get("/api/v1/use_cases")
def get_use_cases():
    """Retrieves a list of all available use cases."""
    try:
        use_cases = [case["use_case"] for case in get_all_use_cases()]
        return {"use_cases": use_cases}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/affected_columns")
def get_affected_columns():
    """Fetches affected columns for all use cases."""
    try:
        affected_columns = {
            case["use_case"]: case["affected_columns"]
            for case in get_all_use_cases()
        }
        return {"affected_columns": affected_columns}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/use_case/{use_case}")
def get_use_case_details(use_case: str):
    """Retrieves details for a specific use case."""
    try:
        all_use_cases = get_all_use_cases()
        
        for case in all_use_cases:
            if case["use_case"] == use_case:
                return {
                    "use_case": case["use_case"],
                    "query": case["query"],
                    "user_input_columns": case["user_input_columns"]
                }
        
        raise HTTPException(status_code=404, detail=f"Use case '{use_case}' not found")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve use case details: {str(e)}")

@app.get("/api/v1/use_case_columns/{use_case}")
def get_use_case_affected_columns(use_case: str):
    """Fetches affected columns and input columns for a specific use case."""
    try:
        all_use_cases = get_all_use_cases()
        
        for case in all_use_cases:
            if case["use_case"] == use_case:
                return {
                    "use_case": case["use_case"],
                    "affected_columns": case["affected_columns"],
                    "user_input_columns": case["user_input_columns"]
                }
        
        raise HTTPException(status_code=404, detail=f"Use case '{use_case}' not found")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve affected columns: {str(e)}")

class UpdateRequest(BaseModel):
    use_case: str
    query: str
    params: List[Any]

@app.post("/api/v1/update_data")
def update_data(request: UpdateRequest):
    """Executes an update query based on the request data."""
    try:
        affected_rows = execute_query(request.query, request.params)
        return {
            "use_case": request.use_case,
            "status": "updated",
            "affected_rows": affected_rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class UseCaseExecutionRequest(BaseModel):
    use_case: str
    user_inputs: Dict[str, Any]  # Example: {"salary": 50000, "employee_id": 123}

@app.post("/api/v1/execute_use_case")
def execute_use_case(request: UseCaseExecutionRequest):
    """Generates and executes an SQL query based on the provided use case and user inputs."""
    try:
        # Extract schema
        schema_extractor = DatabaseSchemaExtractor(CONNECTION_STRING)
        schema = schema_extractor.get_schema()

        # Generate query for the given use case
        query_generator = FinanceQueryGenerator(
            schema=schema,
            api_key=GOOGLE_API_KEY,
            db_type=DB_TYPE,
            model=LLM_MODEL
        )
        generated_query = query_generator.generate_query(request.use_case)

        if not generated_query["query"] or "Error" in generated_query["query"]:
            raise HTTPException(status_code=400, detail="Failed to generate query.")

        # Execute the generated query
        db_executor = DatabaseQueryExecutor(CONNECTION_STRING)
        execution_result = db_executor.execute_query(generated_query["query"], request.user_inputs)

        return {
            "use_case": request.use_case,
            "query": generated_query["query"],
            "user_input_columns": generated_query["user_input_columns"],
            "execution_result": execution_result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
