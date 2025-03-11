from typing import List, Dict, Any
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticOutputParser

class SQLUseCase(BaseModel):
    use_case: str
    query: str
    affected_columns: List[str]
    user_input_columns: List[str]

class SQLUseCaseResponse(BaseModel):
    use_cases: List[SQLUseCase]

class FinanceQueryGenerator:
    def __init__(self, schema: Dict[str, List[str]], api_key: str, db_url: str, db_type: str, model: str = "gemini-1.5-pro"):
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
                Given a database schema, generate business use cases and SQL queries.

                Schema:
                {schema}

                Instructions:
                - Identify business use cases.
                - Generate SQL queries covering retrieval, insertion, updating, deletion.
                - Identify columns needing user input (WHERE, SET, VALUES).
                - Provide 10+ insightful queries.
                - Ensure queries are valid for {self.db_type}.
                
                {sql_syntax_instruction}
                {format_instructions}
            """),
            ("human", "Generate SQL queries covering all relevant business use cases.")
        ])
        
    def generate_use_cases(self) -> List[Dict[str, Any]]:
        try:
            draft_chain = self.draft_prompt | self.llm | self.parser
            draft_result = draft_chain.invoke({"schema": self.schema})

            return [
                {
                    "use_case": item.use_case,
                    "query": item.query,
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
