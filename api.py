from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
import os
from dotenv import load_dotenv
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

@app.get("/api/v1/use_cases")
def get_use_cases():
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

        use_cases = [case["use_case"] for case in query_generator.generate_use_cases()]
        
        return {"use_cases": use_cases}  
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/affected_columns")
def get_affected_columns():
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

        affected_columns = {
            case["use_case"]: case["affected_columns"]
            for case in query_generator.generate_use_cases()
        }
        
        return {"affected_columns": affected_columns}  
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

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


