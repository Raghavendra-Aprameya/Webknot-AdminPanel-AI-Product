from typing import List, Dict, Any
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from pydantic import BaseModel, Field, validator
from langchain_core.output_parsers import PydanticOutputParser

class SQLQueryItem(BaseModel):
    question: str = Field(..., description="Business question this query answers")
    query: str = Field(..., description="SQL query that answers the business question")
    relevance: float = Field(..., ge=0.0, le=1.0, description="Relevance score from 0.0 to 1.0")
    is_time_based: bool = Field(..., description="Whether this query analyzes time-based trends")
    
    @validator('relevance')
    def relevance_must_be_between_0_and_1(cls, v):
        if not 0 <= v <= 1:
            raise ValueError('Relevance must be between 0.0 and 1.0')
        return round(v, 2)

class SQLQueryResponse(BaseModel):
    queries: List[SQLQueryItem] = Field(..., description="List of generated SQL queries")
class FinanceQueryGenerator: 
    def __init__(self, schema: str, api_key: str, model: str = "gemini-1.5-pro"):
        """
        Initialize AI-powered query generator
        
        :param schema: Database schema description
        :param api_key: Google API key
        :param model: LLM model to use
        """
        self.schema = schema
        
        
        self.llm = ChatGoogleGenerativeAI(
            model=model, 
            google_api_key=api_key
        )
        self.parser = PydanticOutputParser(pydantic_object=SQLQueryResponse)
        
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert SQL query generator who creates insightful analytics queries tailored to specific business needs.

              Given a database schema, user role, and business domain, generate 10 high-value SQL queries that would provide meaningful insights.

              Database Schema:
              {schema}

              User Role: {role}
              Business Domain: {domain}

              Instructions:
              - Carefully analyze the schema to identify relevant tables and relationships for the given domain
              - Generate exactly 10 insightful queries:
                * 5 should analyze time-based trends (monthly, quarterly, year-over-year)
                * 5 should provide non-time-based insights (distributions, ratios, aggregations)
              - Each query should directly support decision-making for a {role} in the {domain} context
              - Use appropriate SQL techniques based on the schema structure
              - Assign a relevance score (0.0-1.0) indicating how valuable each query is for the role

              {format_instructions}"""),
                          ("human", "Generate SQL queries for the {role} role in the {domain} domain using the database schema provided.")
        ])
        self.prompt = self.prompt.partial(format_instructions=self.parser.get_format_instructions())

        self.chain = self.prompt | self.llm | self.parser

    def clean_sql(self, sql_query: str) -> str:
        """
        Remove markdown formatting (backticks) from SQL queries.
        """
        return sql_query.replace("```sql", "").replace("```", "").strip()
    def generate_queries(self, role: str="Finance Manager", domain: str = "finance") -> SQLQueryResponse:
        """
        Generate SQL queries tailored to a specific role and domain
        
        :param role: User role (e.g., "Finance Manager", "CFO", "Financial Analyst")
        :param domain: Business domain (e.g., "finance", "retail", "healthcare")
        :return: Structured response with generated SQL queries
        """
        try:
            result = self.chain.invoke({
                "schema": self.schema,
                "role": role,
                "domain": domain
            })
            
            return result
        
        except Exception as e:
            print(f"Error generating queries: {e}")
            return SQLQueryResponse(queries=[
                SQLQueryItem(
                    question="Error generating queries",
                    query=f"-- Error: {str(e)}",
                    relevance=0.0,
                    is_time_based=False
                )
            ])
    
    def get_queries_for_executor(self, role: str="Finance Manager", domain: str="finance") -> List[Dict[str, str]]:

      response = self.generate_queries(role=role, domain=domain)

      return [
          {
              "query": item.query,
              "explanation": item.question,
              "relevance": item.relevance,
              "is_time_based": item.is_time_based
          }
          for item in response.queries
      ]