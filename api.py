
import os
import re
from urllib.parse import quote_plus
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import List, Any
from db_extract import DatabaseSchemaExtractor
from query_generator import FinanceQueryGenerator

# Global variables for database connection
global DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, JDBC_URL, use_case_cache

DB_TYPE = "mysql"
DB_HOST = "127.0.0.1"
DB_PORT = "3306"
DB_NAME = "employee_db"
DB_USER = "root"
DB_PASSWORD = "password"
JDBC_URL = f"{DB_TYPE}+pymysql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
use_case_cache = None

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "AIzaSyCAvX8-4onZYj1oouOLZeKAnDQkdCn4tFs")
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-1.5-pro")

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update with specific origins if necessary
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    """Establish a database connection using updated environment variables."""
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
        return get_all_use_cases()
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
    """Executes an SQL query to update data."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Replace named parameters
        modified_query = replace_named_params(request.query)
        column_names = extract_column_names(modified_query)

        cursor.execute(modified_query, tuple(request.params) if request.params else ())

        if modified_query.strip().lower().startswith("select"):
            results = cursor.fetchall()
        else:
            results = None
            conn.commit()

        cursor.close()
        conn.close()

        return {
            "use_case": request.use_case,
            "query": modified_query,
            "user_input_columns": {col: val for col, val in zip(column_names, request.params)} if column_names else {},
            "execution_result": results if results else f"{cursor.rowcount} rows affected"
        }

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

def replace_named_params(query):
    """Replace named parameters (e.g., :param_name) with %s."""
    return re.sub(r":\w+", "%s", query)

def extract_column_names(query):
    """Extract column names from INSERT and UPDATE queries."""
    insert_match = re.search(r'INSERT INTO\s+\w+\s*\(([^)]+)\)', query, re.IGNORECASE)
    update_match = re.search(r'UPDATE\s+\w+\s+SET\s+([^WHERE]+)', query, re.IGNORECASE)

    if insert_match:
        return [col.strip() for col in insert_match.group(1).split(',')]
    elif update_match:
        return [col.split('=')[0].strip() for col in update_match.group(1).split(',')]
    return []

class DbDetails(BaseModel):
    DB_TYPE: str
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    JDBC_URL: str

# API: Update Database Connection Details
@app.post("/api/v1/dbDetails")
def receive_db_details(details: DbDetails):
    """Update database connection details dynamically and reset cache."""
    try:
        global DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, JDBC_URL, use_case_cache

        # Extract details
        DB_TYPE = details.DB_TYPE
        DB_HOST = details.DB_HOST
        DB_PORT = details.DB_PORT
        DB_NAME = details.DB_NAME
        DB_USER = details.DB_USER
        DB_PASSWORD = details.DB_PASSWORD

        # Fix: Remove "jdbc:" if present
        jdbc_url = details.JDBC_URL.replace("jdbc:", "")
        
        # Add pymysql to MySQL connections
        if DB_TYPE.lower() == "mysql" and "+pymysql" not in jdbc_url:
            JDBC_URL = f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        else:
            JDBC_URL = jdbc_url

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
