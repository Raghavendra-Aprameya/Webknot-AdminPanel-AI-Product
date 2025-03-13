
from typing import List, Dict, Any
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel
from langchain_core.output_parsers import PydanticOutputParser

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

        format_use_case_instructions = self.use_case_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")
        format_query_instructions = self.query_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")

        self.draft_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
                You are an expert SQL query generator.
                Given a database schema, generate different business use cases and SQL queries.

                Schema:
                {schema}

                Instructions:
                - Identify business use cases.
                - Generate SQL queries covering retrieval, insertion, updating, deletion.
                - Identify columns needing user input (WHERE, SET, VALUES).
                - Provide 10+ insightful queries.
                - Ensure queries are valid for {self.db_type}.
                - Use **parameterized query placeholders** like `:parameter_name`.

                {sql_syntax_instruction}
                {format_use_case_instructions}
            """),
            ("human", "Generate SQL queries covering all relevant business use cases.")
        ])

        self.use_case_prompt = ChatPromptTemplate.from_messages([
            ("system", f"""
                You are an expert SQL query generator. 
                Given a use case and database schema, generate the required SQL query.

                Schema:
                {schema}

                Instructions:
                - Understand the given use case.
                - Generate the SQL query for it.
                - Identify input values required from the user.
                - Use parameterized query placeholders (`:param_name`).
                - Ensure queries are valid for {self.db_type}.
                - Output must follow this format:
                
                {format_query_instructions}

                {sql_syntax_instruction}
            """),
            ("human", "Generate an SQL query for the given use case: {use_case}")
        ])

    def generate_use_cases(self) -> List[Dict[str, Any]]:
        try:
            draft_chain = self.draft_prompt | self.llm | self.use_case_parser
            draft_result = draft_chain.invoke({"schema": self.schema})

            return [
                {
                    "use_case": item.use_case,
                    "query": item.query.replace("<", ":").replace(">", ""),
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
                "user_input_columns": {}
            }]

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
