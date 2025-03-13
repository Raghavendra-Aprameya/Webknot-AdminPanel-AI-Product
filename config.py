"""
Configuration settings for the Finance Manager Analytics application.
"""
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD", "Poorvika@3002"))
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("DB_NAME", "employee_db")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_TYPE = os.getenv("DB_TYPE","mysql")

GOOGLE_API_KEY = "AIzaSyCAvX8-4onZYj1oouOLZeKAnDQkdCn4tFs"
LLM_MODEL = os.getenv("LLM_MODEL", "gemini-1.5-pro")

CONNECTION_STRING = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DEFAULT_NUM_QUERIES = int(os.getenv("DEFAULT_NUM_QUERIES", "3"))