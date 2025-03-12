
from typing import List, Dict, Any
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticOutputParser
import re

class SQLUseCase(BaseModel):
    use_case: str
    query: str
    affected_columns: List[str]
    user_input_columns: List[str]

class SQLUseCaseResponse(BaseModel):
    use_cases: List[SQLUseCase]

class FinanceQueryGenerator:
    def __init__(self, schema: str, api_key: str, db_url: str, db_type: str, model: str = "gemini-1.5-pro"):
        self.schema = schema
        self.db_url = db_url
        self.db_type = db_type.lower()
        self.llm = ChatGoogleGenerativeAI(model=model, google_api_key=api_key)
        self.parser = PydanticOutputParser(pydantic_object=SQLUseCaseResponse)

        sql_syntax_instruction = {
            "mysql": "Use MySQL syntax only.",
            "postgres": "Use PostgreSQL syntax only.",
            "sqlite": "Use SQLite syntax only."
        }.get(self.db_type, "Use standard SQL syntax.")

        format_instructions = self.parser.get_format_instructions().replace("{", "{{").replace("}", "}}")

        self.draft_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
                You are an expert SQL query generator.
                Given a database schema, generate a diverse range of business use cases and SQL queries.

                Schema:
                {schema}

                Instructions:
                - Identify varied business use cases across different functional areas (marketing, operations, finance, analytics, customer service).
                - Generate SQL queries covering retrieval, insertion, updating, deletion operations.
                - For each functional area, provide at least 2 distinct business scenarios.
                - Include both operational and analytical queries (daily operations vs. business intelligence).
                - Identify columns needing user input (WHERE, SET, VALUES).
                - Provide 10+ insightful queries with increasing complexity levels (basic, intermediate, advanced).
                - Include at least 2 queries that use CTEs, or advanced joins if appropriate for the schema.
                - Ensure queries are valid for {self.db_type}.
                - Use **parameterized query placeholders** like `:parameter_name` instead of `<parameter_name>`.
                - IMPORTANT: Always include complete comparison operators (>, <, =, >=, <=, <>) in WHERE clauses.
                - When comparing values in WHERE clauses, make sure to write the full expression (e.g., "WHERE salary > :salary" not "WHERE salary :salary")
                - For comparison queries, use the complete syntax (e.g., "WHERE e.salary > m.salary" not "WHERE e.salary m.salary")

                {sql_syntax_instruction}
                {format_instructions}
            """),
            ("human", "Generate a diverse set of SQL queries covering different business functions, operational needs, and analytical requirements.")
        ])
        
    def fix_comparison_operators(self, query: str) -> str:
        """Fix missing comparison operators in SQL queries."""
        # Fix pattern: "column  :param" -> "column > :param" (assuming greater than is intended)
        query = re.sub(r'(\w+)\s{2,}:', r'\1 > :', query)
        
        # Fix pattern: "column  column" -> "column > column" (in joins or comparisons)
        query = re.sub(r'(\w+\.\w+)\s{2,}(\w+\.\w+)', r'\1 > \2', query)
        
        return query
    
    def validate_query(self, query: str) -> str:
        """Validate and fix common SQL syntax errors."""
        # Apply specific fixes
        query = self.fix_comparison_operators(query)
        
        # Fix any malformed joins or where clauses
        query = query.replace("WHERE e.salary m.salary", "WHERE e.salary > m.salary")
        
        # Ensure proper spacing around operators
        query = re.sub(r'(\w+)=(\w+)', r'\1 = \2', query)
        
        return query
    
    def generate_use_cases(self) -> List[Dict[str, Any]]:
        try:
            draft_chain = self.draft_prompt | self.llm | self.parser
            draft_result = draft_chain.invoke({"schema": self.schema})

            return [
                {
                    "use_case": item.use_case,
                    "query": self.validate_query(item.query.replace("<", ":").replace(">", "")),
                    "affected_columns": item.affected_columns,
                    "user_input_columns": item.user_input_columns
                }
                for item in draft_result.use_cases
            ]
        
        except Exception as e:
            return [{
                "use_case": "Error generating queries",
                "query": f"-- Error: {str(e)}",
                "affected_columns": [],
                "user_input_columns": []
            }]