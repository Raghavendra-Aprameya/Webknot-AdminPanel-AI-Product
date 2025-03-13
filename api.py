import re
import os
import mysql.connector
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
from config import CONNECTION_STRING, GOOGLE_API_KEY, LLM_MODEL, DB_TYPE
from db_extract import DatabaseSchemaExtractor
from query_generator import FinanceQueryGenerator

load_dotenv()

# Load database credentials from .env file
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

app = FastAPI()

# Cache for storing generated use cases
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
    
    return use_case_cache  # Categorized dictionary

@app.get("/api/v1/use_cases")
def get_use_cases():
    """Retrieves categorized use cases (Create, Read, Update, Delete)."""
    try:
        return get_all_use_cases()
    except Exception as e:
        return {"status": "error", "message": str(e)}

# @app.get("/api/v1/use_case/{use_case}")
# def get_use_case_details(use_case: str):
#     """Retrieves details for a specific use case."""
#     try:
#         all_use_cases = get_all_use_cases()
        
#         if not isinstance(all_use_cases, dict):
#             raise HTTPException(status_code=500, detail="Use case data is not in the expected format.")

#         for category, use_cases in all_use_cases.items():
#             if isinstance(use_cases, list):
#                 for case in use_cases:
#                     if isinstance(case, dict) and case.get("use_case", "").strip().lower() == use_case.strip().lower():
#                         return {
#                             "use_case": case["use_case"],
#                             "query": case["query"],
#                             "user_input_columns": case.get("user_input_columns", {})
#                         }
        
#         raise HTTPException(status_code=404, detail=f"Use case '{use_case}' not found")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to retrieve use case details: {str(e)}")

class UseCaseRequest(BaseModel):
    use_case: str

@app.post("/api/v1/execute_use_case")
def execute_use_case(request: UseCaseRequest):
    """Generates an SQL query for a given use case without executing it."""
    try:
        schema_extractor = DatabaseSchemaExtractor(CONNECTION_STRING)
        schema = schema_extractor.get_schema()
        
        query_generator = FinanceQueryGenerator(
            schema=schema,
            api_key=GOOGLE_API_KEY,
            db_type=DB_TYPE,
            model=LLM_MODEL
        )
        
        generated_query = query_generator.generate_query(request.use_case)
        
        if not generated_query["query"] or "Error" in generated_query["query"]:
            raise HTTPException(status_code=400, detail="Failed to generate query.")
        
        return {
            "use_case": request.use_case,
            "query": generated_query["query"],
            "user_input_columns": list(generated_query["user_input_columns"].keys())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
class UpdateRequest(BaseModel):
    use_case: str
    query: str
    params: List[Any]

@app.post("/api/v1/update_data")
def update_data(request: UpdateRequest):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # 1. Replace :param_name with %s
        modified_query = replace_named_params(request.query)

        # 2. Extract column names (for logging/debugging purposes)
        column_names = extract_column_names(modified_query)

        # 3. Execute the query
        cursor.execute(modified_query, tuple(request.params) if request.params else ())

        # 4. Handle SELECT results vs. DML operations
        if modified_query.strip().lower().startswith("select"):
            results = cursor.fetchall()  # Fetch results as list of dicts
        else:
            results = None
            conn.commit()

        # 5. Clean up
        cursor.close()
        conn.close()

        # 6. Return response
        return {
            "use_case": request.use_case,
            "query":modified_query,
            "user_input_columns": {col: val for col, val in zip(column_names, request.params)} if column_names else {},
            "execution_result": results if results else f"{cursor.rowcount} rows affected"
        }

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
def replace_named_params(query):
    """
    Replace all :param_name occurrences with %s.
    """
    # This regex looks for : followed by word characters and replaces with %s
    return re.sub(r":\w+", "%s", query)

def extract_column_names(query):
    insert_match = re.search(r'INSERT INTO\s+\w+\s*\(([^)]+)\)', query, re.IGNORECASE)
    update_match = re.search(r'UPDATE\s+\w+\s+SET\s+([^WHERE]+)', query, re.IGNORECASE)
    
    if insert_match:
        return [col.strip() for col in insert_match.group(1).split(',')]
    elif update_match:
        return [col.split('=')[0].strip() for col in update_match.group(1).split(',')]
    return []