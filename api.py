from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import mysql.connector
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from config import CONNECTION_STRING, GOOGLE_API_KEY, LLM_MODEL, DB_TYPE
from db_extract import DatabaseSchemaExtractor
from query_generator import FinanceQueryGenerator

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

app = FastAPI()

class UpdateRequest(BaseModel):
    use_case: str
    query: str
    params: list

# Cache for storing generated use cases to avoid regenerating them for each request
use_case_cache = None

def get_db_connection():
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def execute_query(query, params=None):
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
    """Helper function to get all use cases and cache them"""
    global use_case_cache
    
    if use_case_cache is None:
        try:
            schema_extractor = DatabaseSchemaExtractor(CONNECTION_STRING)
            schema = schema_extractor.get_schema()
            
            query_generator = FinanceQueryGenerator(
                schema=schema,
                api_key=GOOGLE_API_KEY,
                db_url=CONNECTION_STRING,
                db_type=DB_TYPE,
                model=LLM_MODEL
            )
            
            use_case_cache = query_generator.generate_use_cases()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to generate use cases: {str(e)}")
    
    return use_case_cache

@app.get("/api/v1/use_cases")
def get_use_cases():
    try:
        use_cases = [case["use_case"] for case in get_all_use_cases()]
        return {"use_cases": use_cases}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/affected_columns")
def get_affected_columns():
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
    try:
        all_use_cases = get_all_use_cases()
        
        # Find the specific use case
        for case in all_use_cases:
            if case["use_case"] == use_case:
                return {
                    "use_case": case["use_case"],
                    "query": case["query"],
                    # "affected_columns": case["affected_columns"],
                    "user_input_columns": case["user_input_columns"]
                }
        
        # If the use case is not found
        raise HTTPException(status_code=404, detail=f"Use case '{use_case}' not found")
    
    except HTTPException as e:
        raise e  # Re-raise HTTP exceptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve use case details: {str(e)}")

@app.get("/api/v1/use_case_columns/{use_case}")
def get_use_case_affected_columns(use_case: str):
    try:
        all_use_cases = get_all_use_cases()
        
        # Find the specific use case
        for case in all_use_cases:
            if case["use_case"] == use_case:
                return {
                    "use_case": case["use_case"],
                    "affected_columns": case["affected_columns"],
                    "user_input_columns": case["user_input_columns"]
                }
        
        # If the use case is not found
        raise HTTPException(status_code=404, detail=f"Use case '{use_case}' not found")
    
    except HTTPException as e:
        raise e  # Re-raise HTTP exceptions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve affected columns: {str(e)}")

@app.post("/api/v1/update_data")
def update_data(request: UpdateRequest):
    try:
        affected_rows = execute_query(request.query, request.params)
        return {
            "use_case": request.use_case,
            "status": "updated",
            "affected_rows": affected_rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))