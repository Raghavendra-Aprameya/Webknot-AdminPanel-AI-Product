# from typing import List, Dict, Any
# import google.generativeai as genai
# from langchain_core.prompts import ChatPromptTemplate
# from langchain_google_genai import ChatGoogleGenerativeAI
# from pydantic import BaseModel
# from langchain_core.output_parsers import PydanticOutputParser

# class SQLUseCase(BaseModel):
#     use_case: str
#     query: str
#     affected_columns: List[str]
#     user_input_columns: Dict[str, str]  # { "column_name": "data_type" }

# class SQLUseCaseResponse(BaseModel):
#     use_cases: List[SQLUseCase]

# class FinanceQueryGenerator:
#     def __init__(self, schema: str, api_key: str, db_type: str, model: str = "gemini-1.5-pro"):
#         self.schema = schema
#         self.db_type = db_type.lower()
#         self.llm = ChatGoogleGenerativeAI(model=model, google_api_key=api_key)
#         self.use_case_parser = PydanticOutputParser(pydantic_object=SQLUseCaseResponse)
#         self.query_parser = PydanticOutputParser(pydantic_object=SQLUseCase)

#         sql_syntax_instruction = {
#             "mysql": "Use MySQL syntax only.",
#             "postgres": "Use PostgreSQL syntax only.",
#             "sqlite": "Use SQLite syntax only."
#         }.get(self.db_type, "Use standard SQL syntax.")

#         format_use_case_instructions = self.use_case_parser.get_format_instructions().replace("{", "{{").replace("}", "}}").replace("[", "").replace("]", "")
#         format_query_instructions = self.query_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")

#         self.draft_prompt = ChatPromptTemplate.from_messages([
#             ("system", f"""
#             You are an expert SQL query generator.
#             Given a database schema, generate **EXACTLY** 20 use cases:  
#             - **5 for Creating data (INSERT queries)**  
#             - **5 for Reading data (SELECT queries)**  
#             - **5 for Updating data (UPDATE queries)**  
#             - **5 for Deleting data (DELETE queries)**  

#             **Schema:**
#             {schema}

#             **Instructions:**
#             - Clearly categorize use cases as "Create", "Read", "Update", or "Delete".
#             - Generate meaningful queries covering different business cases.
#             - Identify which columns require user input.
#             - Ensure queries follow {self.db_type} syntax.
#             - Use **parameterized placeholders** (`:param_name`) instead of raw values.

#             **Output Format:**
#             {format_use_case_instructions}
#             """),
#             ("human", "Generate SQL queries categorized into Create, Read, Update, and Delete.")
#         ])

#         self.use_case_prompt = ChatPromptTemplate.from_messages([
#             ("system", f"""
#                 You are an expert SQL query generator. 
#                 Given a use case and database schema, generate the required SQL query.

#                 Schema:
#                 {schema}

#                 **Instructions:**
#                 - Understand the given use case.
#                 - Generate the SQL query for it.
#                 - Identify input values required from the user.
#                 - Use parameterized query placeholders (`:param_name`).
#                 - Ensure queries are valid for {self.db_type}.
#                 - Output must follow this format:

#                 {format_query_instructions}   

#                 {sql_syntax_instruction}
#             """),
#             ("human", "Generate an SQL query for the given use case: {use_case}")
#         ])

#     def generate_use_cases(self) -> Dict[str, List[str]]:
#         """Generates and categorizes use cases into Create, Read, Update, and Delete."""
#         try:
#             draft_chain = self.draft_prompt | self.llm | self.use_case_parser
#             draft_result = draft_chain.invoke({"schema": self.schema})

#             categorized_use_cases = {
#                 "Create": [],
#                 "Read": [],
#                 "Update": [],
#                 "Delete": []
#             }

#             for item in draft_result.use_cases:
#                 use_case_text = item.use_case.lower()

#                 if any(word in use_case_text for word in ["create", "add", "insert", "register"]):
#                     categorized_use_cases["Create"].append(item.use_case)
#                 elif any(word in use_case_text for word in ["get", "fetch", "retrieve", "list", "find", "view", "show", "display", "select"]):
#                     categorized_use_cases["Read"].append(item.use_case)
#                 elif any(word in use_case_text for word in ["update", "modify", "change", "edit"]):
#                     categorized_use_cases["Update"].append(item.use_case)
#                 elif any(word in use_case_text for word in ["delete", "remove", "erase"]):
#                     categorized_use_cases["Delete"].append(item.use_case)

#             return categorized_use_cases

#         except Exception as e:
#             return {"error": f"Failed to generate use cases: {str(e)}"}

#     def generate_query(self, use_case: str) -> Dict[str, Any]:
#         """Generates an SQL query based on the provided use case."""
#         try:
#             query_chain = self.use_case_prompt | self.llm | self.query_parser
#             query_result = query_chain.invoke({"use_case": use_case, "schema": self.schema})

#             return {
#                 "query": query_result.query.replace("<", ":").replace(">", ""),
#                 "user_input_columns": query_result.user_input_columns
#             }

#         except Exception as e:
#             return {
#                 "query": f"-- Error: {str(e)}",
#                 "user_input_columns": {}
#             }
from typing import List, Dict, Any
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel
from langchain_core.output_parsers import PydanticOutputParser
import re

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
            "postgres": "Use PostgreSQL syntax only.",
            "sqlite": "Use SQLite syntax only."
        }.get(self.db_type, "Use standard SQL syntax.")

        format_use_case_instructions = self.use_case_parser.get_format_instructions().replace("{", "{{").replace("}", "}}").replace("[", "").replace("]", "")
        format_query_instructions = self.query_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")

        self.draft_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
    You are an expert SQL query generator.
    Given a database schema, generate **EXACTLY** 20 complex use cases:  
    - **5 for Creating data (INSERT queries)**  
    - **5 for Reading data (SELECT queries)**  
    - **5 for Updating data (UPDATE queries)**  
    - **5 for Deleting data (DELETE queries)**  

    **Schema:**
    {schema}

    **Instructions:**
    
    - **Generate complex queries** that involve:
      - **Joins (INNER, LEFT, RIGHT, FULL)**
      - **Subqueries**
      - **Aggregate functions (SUM, COUNT, AVG, MAX, MIN)**
      - **Window functions (ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG)**
      - **CTEs (WITH statements)**
    - Identify which columns require user input.
    - Ensure queries follow {self.db_type} syntax.
    - Use **parameterized placeholders** (:param_name) instead of raw values.

    **Output Format:**
    {format_use_case_instructions}
    """),
    ("human", "Generate complex SQL queries categorized into Create, Read, Update, and Delete.")
])

        self.use_case_prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
        You are an expert SQL query generator. 
        Given a use case and database schema, generate the required SQL query.

        **Schema:**
        {schema}

        **Instructions:**
        - First, check if the use case is related to a **database operation**.
          - If not, return: **"The useCase is not valid."**
        - If the query involves **DDL operations** (CREATE, ALTER, DROP, TRUNCATE), return:
          - **"DDL operations are not allowed. Please provide a valid DML use case."**
        - Generate **only DML queries** (INSERT, SELECT, UPDATE, DELETE).
        - Ensure queries use:
          - **Joins (INNER, LEFT, RIGHT, FULL)**
          - **Subqueries**
          - **Aggregate functions (SUM, COUNT, AVG, MAX, MIN)**
          - **Window functions (ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG)**
          - **CTEs (WITH statements)**
        - Use parameterized placeholders (`:param_name`).
        - Ensure queries are valid for {self.db_type}.
        - Output must follow this format:

        {format_query_instructions}

        {sql_syntax_instruction}
    """),
    ("human", "Generate an SQL query for the given use case: {use_case}")
])



    def generate_query(self, use_case: str) -> Dict[str, Any]:
        """Generates an SQL query based on the provided use case."""
        try:
            query_chain = self.use_case_prompt | self.llm | self.query_parser
            query_result = query_chain.invoke({"use_case": use_case, "schema": self.schema})
        
            query_text = query_result.query.replace("<", ":").replace(">", "")
        
        # Check if the query is a valid SQL statement
            dml_keywords = ["select", "insert", "update", "delete"]
            is_valid = False
        
        # First, check if the query starts with a valid DML keyword
            if any(query_text.lower().strip().startswith(keyword) for keyword in dml_keywords):
                is_valid = True
        
        # Check for phrases that indicate invalid responses
            invalid_phrases = [
                "not valid", 
                "invalid", 
                "can't generate", 
                "cannot generate",
                "error",
            "unable to",
            "ddl operations",  # Catch DDL operation error messages
            "not allowed",
            "please provide",
            "use case is"
        ]
        
        # If any invalid phrase is found, mark as invalid
            if any(phrase in query_text.lower() for phrase in invalid_phrases):
                is_valid = False
        
            return {
            "valid": is_valid,
            "query": query_text,
            "user_input_columns": query_result.user_input_columns
        }

        except Exception as e:
            return {
            "valid": False,
            "query": f"-- Error: {str(e)}",
            "user_input_columns": {}
        }