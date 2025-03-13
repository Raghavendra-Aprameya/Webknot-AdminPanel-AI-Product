
import re
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

def extract_column_names(query):
    insert_match = re.search(r'INSERT INTO\s+\w+\s*\(([^)]+)\)', query, re.IGNORECASE)
    update_match = re.search(r'UPDATE\s+\w+\s+SET\s+([^WHERE]+)', query, re.IGNORECASE)
    
    if insert_match:
        return [col.strip() for col in insert_match.group(1).split(',')]
    elif update_match:
        return [col.split('=')[0].strip() for col in update_match.group(1).split(',')]
    return []

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
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        column_names = extract_column_names(request.query)
        cursor.execute(request.query, tuple(request.params) if request.params else ())

        if request.query.strip().lower().startswith("select"):
            results = cursor.fetchall()  # Fetch results as a list of dictionaries
        else:
            results = None
            conn.commit()

        cursor.close()
        conn.close()

        return {
            "use_case": request.use_case,
            "query": request.query,
            "user_input_columns": {col: val for col, val in zip(column_names, request.params)} if column_names else {},
            "execution_result": {
                "results": results if results else f"{cursor.rowcount} rows affected"
            }
        }

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")



class UseCaseRequest(BaseModel):
    use_case: str  # Only use_case is required in the request body

@app.post("/api/v1/execute_use_case")
def execute_use_case(request: UseCaseRequest):
    """Generates an SQL query based on the provided use case and returns it without execution."""
    try:
        # Extract schema from the database
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

        # Convert user_input_columns dict keys to a list
        user_input_columns = list(generated_query["user_input_columns"].keys())

        return {
            "use_case": request.use_case,
            "query": generated_query["query"],
            "user_input_columns": user_input_columns
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))