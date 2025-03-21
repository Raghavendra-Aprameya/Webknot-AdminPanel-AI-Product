
import os
import re
from urllib.parse import quote_plus
import mysql.connector
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, inspect, MetaData, text
from sqlalchemy.orm import sessionmaker
import uuid
from db_extract import DatabaseSchemaExtractor
from query_generator import FinanceQueryGenerator
from query_exec import DatabaseQueryExecutor  # Import the new class

# Load environment variables
load_dotenv()

# Global variables for database connection
global DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, JDBC_URL, use_case_cache
use_case_cache = None  # Initialize the cache

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "AIzaSyCAvX8-4onZYj1oouOLZeKAnDQkdCn4tFs")
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-1.5-pro")

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_connection():
    """Establish a database connection using updated environment variables."""
    try:
        # Handle the case where DB_PORT might be None
        port_number = int(DB_PORT) if DB_PORT else (3306 if DB_TYPE.lower() == "mysql" else 5432)
        
        if DB_TYPE.lower() == "mysql":
            return mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                port=port_number
            )
        elif DB_TYPE.lower() in ["postgresql", "postgres"]:
            return psycopg2.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
                port=port_number
            )
        else:
            raise HTTPException(status_code=500, detail="Unsupported database type.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")


# def get_all_use_cases():
#     """Fetches and caches all use cases using updated database connection details."""
#     global use_case_cache, JDBC_URL, DB_TYPE  # Ensure global variables are used

#     if use_case_cache is None:
#         try:
#             schema_extractor = DatabaseSchemaExtractor(JDBC_URL)
#             schema = schema_extractor.get_schema()

#             query_generator = FinanceQueryGenerator(
#                 schema=schema,
#                 api_key=GOOGLE_API_KEY,
#                 db_type=DB_TYPE,
#                 model=LLM_MODEL
#             )

#             use_case_cache = query_generator.generate_use_cases()
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Failed to generate use cases: {str(e)}")

#     return use_case_cache
def get_all_use_cases():
    """Fetches and caches all use cases using updated database connection details."""
    global use_case_cache, JDBC_URL, DB_TYPE  # Ensure global variables are used

    if use_case_cache is None:
        try:
            schema_extractor = DatabaseSchemaExtractor(JDBC_URL)
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
    """Retrieve categorized use cases."""
    try:
        use_cases = get_all_use_cases()
        return {
            "status": "success",
            "use_cases_result": use_cases
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

class UseCaseRequest(BaseModel):
    use_case: str

@app.post("/api/v1/execute_use_case")
def execute_use_case(request: UseCaseRequest):
    """Generates an SQL query for a given use case."""
    try:
        schema_extractor = DatabaseSchemaExtractor(JDBC_URL)
        schema = schema_extractor.get_schema()

        query_generator = FinanceQueryGenerator(
            schema=schema,
            api_key=GOOGLE_API_KEY,
            db_type=DB_TYPE,
            model=LLM_MODEL
        )

        result = query_generator.generate_query(request.use_case)
        
        # Additional validation for non-SQL responses
        query_text = result["query"].lower()
        dml_keywords = ["select", "insert", "update", "delete"]
        invalid_phrases = [
            "not valid", 
            "invalid", 
            "can't generate", 
            "cannot generate",
            "error",
            "unable to",
            "ddl operations",
            "not allowed",
            "please provide",
            "use case is"
        ]
        
        # Check if the query starts with a valid DML keyword
        starts_with_dml = any(query_text.strip().startswith(keyword) for keyword in dml_keywords)
        
        # Check if the query contains any invalid phrases
        contains_invalid_phrase = any(phrase in query_text for phrase in invalid_phrases)
        
        # Determine validity
        is_valid = starts_with_dml and not contains_invalid_phrase
        
        return {
            "valid": is_valid,
            "use_case": request.use_case,
            "query": result["query"],
            "user_input_columns": list(result["user_input_columns"].keys()) if isinstance(result["user_input_columns"], dict) else []
        }
    except Exception as e:
        return {
            "valid": False,
            "use_case": request.use_case,
            "query": f"Error: {str(e)}",
            "user_input_columns": []
        }

@app.get("/api/v1/use_cases")
def get_use_cases():
    """Retrieve categorized use cases."""
    try:
        use_cases = get_all_use_cases()
        return {
            "status": "success",
            "use_cases_result": use_cases
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# @app.get("/api/v1/use_cases")
# def get_use_cases():
#     """Retrieve categorized use cases."""
#     try:
#         use_cases = get_all_use_cases()
#         return {
#             "status": "success",
#             "use_cases_result": use_cases
#         }
#     except Exception as e:
#         return {"status": "error", "message": str(e)}


class DbDetails(BaseModel):
    DB_TYPE: str
    DB_HOST: str
    DB_PORT: Optional[str] = None  # Make it optional with a default of None
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str


@app.post("/api/v1/dbDetails")
def receive_db_details(details: DbDetails):
    """Update database connection details dynamically and reset cache."""
    try:
        global DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, JDBC_URL, use_case_cache

        DB_TYPE = details.DB_TYPE.lower()
        DB_HOST = details.DB_HOST
        DB_NAME = details.DB_NAME
        DB_USER = details.DB_USER
        DB_PASSWORD = details.DB_PASSWORD

        # Set default ports if not provided
        if not details.DB_PORT:
            DB_PORT = "3306" if DB_TYPE == "mysql" else "5432"
        else:
            # Validate port if provided
            if not details.DB_PORT.isdigit():
                raise HTTPException(status_code=400, detail="Invalid port number")
            DB_PORT = details.DB_PORT

        # Adjust JDBC_URL based on database type
        if DB_TYPE == "mysql":
            JDBC_URL = f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        elif DB_TYPE in ["postgresql", "postgres"]:
            JDBC_URL = f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported database type.")

        print(f"Updated JDBC_URL: {JDBC_URL}")  # Debugging

        # Reset use case cache
        use_case_cache = None  

        return {
            "status": "success",
            "message": "Database details updated",
            "JDBC_URL": JDBC_URL    
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update database details: {str(e)}",
        }


class UpdateRequest(BaseModel):
    use_case: str
    query: str
    params: Optional[List[Any]] = None

@app.post("/api/v1/update_data")
def update_data(request: UpdateRequest):
    """Executes an SQL query to update data."""
    try:
        # Initialize the database executor
        executor = DatabaseQueryExecutor(JDBC_URL)
        
        # Extract column names for the response
        column_names = extract_column_names(request.query)
        
        # Execute the query
        result = executor.execute_query(request.query, request.params or [])
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        
        # Build the response
        response = {
            "use_case": request.use_case,
            "query": request.query,
            "user_input_columns": {col: val for col, val in zip(column_names, request.params or [])} if column_names else {}
        }
        
        # Add appropriate result data to the response
        if result.get("type") == "select":
            response["execution_result"] = f"{result['count']} record(s) found."
            response["data"] = result["results"]  # Add the actual data
        else:
            response["execution_result"] = result["results"]
            
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

def extract_column_names(query):
    """Extract column names from INSERT and UPDATE queries."""
    insert_match = re.search(r'INSERT INTO\s+\w+\s*\(([^)]+)\)', query, re.IGNORECASE)
    update_match = re.search(r'UPDATE\s+\w+\s+SET\s+([^WHERE]+)', query, re.IGNORECASE)

    if insert_match:
        return [col.strip() for col in insert_match.group(1).split(',')]
    elif update_match:
        return [col.split('=')[0].strip() for col in update_match.group(1).split(',')]
    return []


class UseCaseRequest(BaseModel):
    use_case: str


# @app.post("/api/v1/execute_use_case")
# def execute_use_case(request: UseCaseRequest):
#     """Generates an SQL query for a given use case, ensuring compatibility with both MySQL and PostgreSQL."""
#     try:
#         if DB_TYPE.lower() not in ["mysql", "postgresql", "postgres"]:
#             raise HTTPException(status_code=400, detail="Unsupported database type.")

#         schema_extractor = DatabaseSchemaExtractor(JDBC_URL)
#         schema = schema_extractor.get_schema()

#         if not schema:
#             raise HTTPException(status_code=500, detail="Schema extraction failed.")

#         query_generator = FinanceQueryGenerator(
#             schema=schema,
#             api_key=GOOGLE_API_KEY,
#             db_type=DB_TYPE,
#             model=LLM_MODEL
#         )

#         result = query_generator.generate_query(request.use_case)

#         query_text = result["query"].strip().lower()
#         dml_keywords = ["select", "insert", "update", "delete"]
#         invalid_phrases = [
#             "not valid", "invalid", "can't generate", "cannot generate",
#             "error", "unable to", "ddl operations", "not allowed",
#             "please provide", "use case is"
#         ]

#         starts_with_dml = any(query_text.startswith(keyword) for keyword in dml_keywords)
#         contains_invalid_phrase = any(phrase in query_text for phrase in invalid_phrases)
#         is_valid = starts_with_dml and not contains_invalid_phrase

#         return {
#             "valid": is_valid,
#             "use_case": request.use_case,
#             "query": result["query"],
#             "user_input_columns": list(result["user_input_columns"].keys()) if isinstance(result["user_input_columns"], dict) else []
#         }

#     except Exception as e:
#         return {
#             "valid": False,
#             "use_case": request.use_case,
#             "query": f"Error: {str(e)}",
#             "user_input_columns": []
#         }