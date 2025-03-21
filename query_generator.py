
from typing import List, Dict, Any
from fastapi import HTTPException
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel
from langchain_core.output_parsers import PydanticOutputParser
import re
import uuid
class SQLUseCase(BaseModel):
    use_case: str
    query: str
    affected_columns: List[str]
    user_input_columns: Dict[str, str]  # { "column_name": "data_type" }

class SQLUseCaseResponse(BaseModel):
    use_cases: List[SQLUseCase]

class FinanceQueryGenerator:
    def __init__(self, schema: str, api_key: str, db_type: str, model: str = "gemini-1.5-pro"):
        self.schema = schema
        self.db_type = db_type.lower()
        self.llm = ChatGoogleGenerativeAI(model=model, google_api_key=api_key)
        self.use_case_parser = PydanticOutputParser(pydantic_object=SQLUseCaseResponse)
        self.query_parser = PydanticOutputParser(pydantic_object=SQLUseCase)

        sql_syntax_instruction = {
            "mysql": "Use MySQL syntax only.",
            "postgres": "Use PostgreSQL syntax only. Use double quotes for column names, single quotes for values. Use 'SERIAL' for auto-increment. Use RETURNING * for returning data after INSERT/UPDATE.",
            "sqlite": "Use SQLite syntax only."
        }.get(self.db_type, "Use standard SQL syntax.")


        format_use_case_instructions = self.use_case_parser.get_format_instructions().replace("{", "{{").replace("}", "}}").replace("[", "").replace("]", "")
        format_query_instructions = self.query_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")

        self.draft_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
    You are an expert SQL query generator specializing in business database operations.
    Given a database schema, generate **EXACTLY** 20 business-relevant use cases:  
    - **5 for Creating data (INSERT queries)** - focusing on adding new business records  
    - **5 for Reading data (SELECT queries)** - focusing on business insights and reporting  
    - **5 for Updating data (UPDATE queries)** - focusing on maintaining accurate business data  
    - **5 for Deleting data (DELETE queries)** - focusing on data cleanup and compliance  

    **Schema:**
    {schema}

    **Instructions:**
    
    - Generate  queries for **real-world business scenarios** that involve:
      - **Joins (INNER, LEFT, RIGHT, FULL)** to connect related business entities
    - Identify which columns require user input for business operations.
    - Ensure queries follow {self.db_type} syntax.
    - Use **parameterized placeholders** (:param_name) instead of raw values.
    - Focus on **administrative and management tasks** relevant to business owners.
    - IMPORTANT: Generate use cases for manipulating DATA ONLY, not database STRUCTURE.
    - Do NOT include use cases for creating/altering/dropping tables, indexes, or constraints.
    - Make sure the use casews are related to schema provided.

    **Output Format:**
    {format_use_case_instructions}
    """),
    ("human", "Generate complex SQL queries for real-world business use cases categorized into Create, Read, Update, and Delete operations.")
])
        self.use_case_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
        You are an expert SQL query generator specializing in business database operations. 
        Given a use case and database schema, generate the required SQL query.

        **Schema:**
        {schema}

        **Instructions:**
        - First, analyze if the use case is related to a **database operation with existing tables**.
          - If not, return: **"The useCase is not valid for business operations."**
        
        - IMPORTANT: Check if the use case requests structural changes to the database:
          - If it contains terms like "create table", "add column", "alter table", "drop table", 
            "add constraint", "create index", or any other DDL operation, return:
          - **"This request involves database structure changes (DDL operations), which are not allowed. 
             Please provide a use case for manipulating business data within existing tables."**
          
        - Understand the business context:
          - "Add employee" means INSERT into employee table (valid)
          - "Add employee table" means CREATE TABLE (invalid)
        
        - Generate **only DML queries** (INSERT, SELECT, UPDATE, DELETE) that work with business data.
        - Create queries that help business owners manage their data and gain insights.
        - Ensure queries use:
          - **Joins (INNER, LEFT, RIGHT, FULL)** to connect related business entities
          
        - Use parameterized placeholders (:param_name).
        - Ensure queries are valid for {self.db_type}.
        - Output must follow this format:

        {format_query_instructions}

        {sql_syntax_instruction}
    """),
    ("human", "Generate an SQL query for the following business operation: {use_case}")
])


    # def generate_use_cases(self) -> Dict[str, List[str]]:
    #     """Generates and categorizes use cases into Create, Read, Update, and Delete."""
    #     try:
    #         draft_chain = self.draft_prompt | self.llm | self.use_case_parser
    #         draft_result = draft_chain.invoke({"schema": self.schema})

    #         categorized_use_cases = {
    #             "Create": [],
    #             "Read": [],
    #             "Update": [],
    #             "Delete": []
    #         }

    #         for item in draft_result.use_cases:
    #             use_case_text = item.use_case.lower()

    #             if any(word in use_case_text for word in ["create", "add", "insert", "register"]):
    #                 categorized_use_cases["Create"].append(item.use_case)
    #             elif any(word in use_case_text for word in ["get", "fetch", "retrieve", "list", "find", "view", "show", "display", "select"]):
    #                 categorized_use_cases["Read"].append(item.use_case)
    #             elif any(word in use_case_text for word in ["update", "modify", "change", "edit"]):
    #                 categorized_use_cases["Update"].append(item.use_case)
    #             elif any(word in use_case_text for word in ["delete", "remove", "erase"]):
    #                 categorized_use_cases["Delete"].append(item.use_case)

    #         return categorized_use_cases

    #     except Exception as e:
    #         return {"error": f"Failed to generate use cases: {str(e)}"}
    def generate_use_cases(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generates exactly 5 use cases for each operation (Create, Read, Update, Delete).
        Returns a dictionary with use cases categorized into Create, Read, Update, and Delete."""
        try:
            use_case_chain = self.draft_prompt | self.llm | self.use_case_parser
            use_case_result = use_case_chain.invoke({"schema": self.schema})

            
            create_use_cases = []
            read_use_cases = []
            update_use_cases = []
            delete_use_cases = []

            for use_case in use_case_result.use_cases:
                use_case_id = str(uuid.uuid4())
                user_input_columns = []

                
                query_lower = use_case.query.lower().strip()
                
                if query_lower.startswith("insert"):
                    
                    match = re.search(r"INSERT INTO \w+ \((.*?)\)", use_case.query, re.IGNORECASE)
                    if match:
                        columns = match.group(1).split(",")
                        user_input_columns = [col.strip() for col in columns]
                
                elif query_lower.startswith("update"):
                    
                    match = re.search(r"UPDATE \w+ SET (.*?)(?: WHERE|$)", use_case.query, re.IGNORECASE)
                    if match:
                        set_clause = match.group(1)
                        columns = [col.split("=")[0].strip() for col in set_clause.split(",")]
                        user_input_columns = columns
                
                elif query_lower.startswith("delete"):
                    
                    match = re.search(r"DELETE FROM \w+ WHERE (.*?)(?:$|;)", use_case.query, re.IGNORECASE)
                    if match:
                        where_clause = match.group(1)
                        
                        columns = [col.split("=")[0].strip() for col in where_clause.split(" AND ")]
                        user_input_columns = columns

                use_case_data = {
                    "use_case_id": use_case_id,  
                    "use_case": use_case.use_case,
                    "query": use_case.query,
                    "user_input_columns": user_input_columns  
                }

                
                if query_lower.startswith("insert") and len(create_use_cases) < 5:
                    create_use_cases.append(use_case_data)
                elif query_lower.startswith("select") and len(read_use_cases) < 5:
                    read_use_cases.append(use_case_data)
                elif query_lower.startswith("update") and len(update_use_cases) < 5:
                    update_use_cases.append(use_case_data)
                elif query_lower.startswith("delete") and len(delete_use_cases) < 5:
                    delete_use_cases.append(use_case_data)

                
                if (
                    len(create_use_cases) == 5
                    and len(read_use_cases) == 5
                    and len(update_use_cases) == 5
                    and len(delete_use_cases) == 5
                ):
                    break

            return {
                "create": create_use_cases,
                "read": read_use_cases,
                "update": update_use_cases,
                "delete": delete_use_cases
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to generate use cases: {str(e)}")

    def generate_query(self, use_case: str) -> Dict[str, Any]:
        """Generates an SQL query based on the provided use case."""
        try:
            query_chain = self.use_case_prompt | self.llm | self.query_parser
            query_result = query_chain.invoke({"use_case": use_case, "schema": self.schema})

            return {
                "query": query_result.query.replace("<", ":").replace(">", ""),
                "user_input_columns": query_result.user_input_columns
            }

        except Exception as e:
            return {
                "query": f"-- Error: {str(e)}",
                "user_input_columns": {}
            }