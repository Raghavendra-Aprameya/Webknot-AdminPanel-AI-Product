from typing import List, Dict, Any
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticOutputParser
import re

class SQLUseCase(BaseModel):
    use_case: str = Field(..., description="Business use case this query addresses")
    query: str = Field(..., description="SQL query that implements the use case")
    affected_columns: List[str] = Field(..., description="List of columns involved in the query, prefixed with table names")

class SQLUseCaseResponse(BaseModel):
    use_cases: List[SQLUseCase] = Field(..., description="Generated SQL queries with relevant use cases")

class FinanceQueryGenerator: 
    def __init__(self, schema: str, api_key: str, db_url: str, db_type: str, model: str = "gemini-1.5-pro"):
        """
        Initialize AI-powered query generator.
        
        :param schema: Database schema description
        :param api_key: Google API key
        :param db_url: Database connection URL
        :param db_type: Type of database (MySQL, PostgreSQL, etc.)
        :param model: LLM model to use
        """
        self.schema = schema
        self.db_url = db_url
        self.db_type = db_type.lower()
        self.llm = ChatGoogleGenerativeAI(
            model=model, 
            google_api_key=api_key
        )
        self.parser = PydanticOutputParser(pydantic_object=SQLUseCaseResponse)

        sql_syntax_instruction = {
            "mysql": "Use MySQL syntax only.",
            "postgres": "Use PostgreSQL syntax only.",
            "sqlite": "Use SQLite syntax only."
        }.get(self.db_type, "Use standard SQL syntax.")

        # ✅ FIX: Escape `{{ }}` to avoid missing variables error
        format_instructions = self.parser.get_format_instructions()
        format_instructions_escaped = format_instructions.replace("{", "{{").replace("}", "}}")

        self.draft_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""You are an expert SQL query generator.

                Given a database schema, generate all possible business use cases along with corresponding SQL queries.

                Schema:
                {schema}

                Instructions:
                - Identify possible use cases based on table structures and relationships.
                - Generate queries that match common business operations (retrieval, insertion, updating, deletion, analytics).
                - For each query, list the column names that are affected, prefixed with table names.
                - Provide 10+ insightful queries across different categories.
                - Ensure queries are valid for {self.db_type}.
                
                {sql_syntax_instruction}
                {format_instructions_escaped}  # ✅ Fix: Use escaped instructions
            """),
            ("human", "Generate SQL queries covering all relevant business use cases.")
        ])
        
    def generate_use_cases(self) -> List[Dict[str, Any]]:
        """
        Generate SQL queries covering all relevant use cases based on schema.
        
        :return: List of dictionaries with use cases, queries, and affected columns
        """
        try:
            draft_chain = self.draft_prompt | self.llm | self.parser
            draft_result = draft_chain.invoke({
                "schema": self.schema
            })

            return [
                {
                    "use_case": item.use_case,
                    "query": item.query,
                    "affected_columns": item.affected_columns
                }
                for item in draft_result.use_cases
            ]
        
        except Exception as e:
            print(f"Error generating use cases: {e}")
            return [{
                "use_case": "Error generating queries",
                "query": f"-- Error: {str(e)}",
                "affected_columns": []
            }]
