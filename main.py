import traceback
from config import CONNECTION_STRING, GOOGLE_API_KEY, LLM_MODEL, DB_TYPE
from db_extract import DatabaseSchemaExtractor
from query_exec import DatabaseQueryExecutor
from query_generator import FinanceQueryGenerator

def main():
    try:
        print("🔍 Extracting database schema...")
        schema_extractor = DatabaseSchemaExtractor(CONNECTION_STRING)
        schema = schema_extractor.get_schema()
        print("✅ Schema extracted successfully!")

        print("\n🤖 Generating finance-related queries using AI...")
        query_generator = FinanceQueryGenerator(
            schema=schema,
            api_key=GOOGLE_API_KEY,
            db_url=CONNECTION_STRING,
            db_type=DB_TYPE,
            model=LLM_MODEL
        )

        finance_queries = query_generator.generate_use_cases()
        
        if not finance_queries:
            print("⚠️ No queries generated. Please check the schema or LLM configuration.")
            return

        print(f"✅ Generated {len(finance_queries)} finance queries.")

        print("\n⚡ Executing queries...")
        query_executor = DatabaseQueryExecutor(CONNECTION_STRING)
        query_results = query_executor.execute_queries(finance_queries)

        print("\n📊 === Generated Queries & Use Cases ===")
        for result in query_results:
            print(f"\n💡 Use Case: {result['use_case']}")
            print(f"📌 SQL Query:\n{result['query']}")
            print(f"⚠️ Affected Columns: {', '.join(result['affected_columns']) if result['affected_columns'] else 'None'}")

    except Exception as e:
        print(f"\n🚨 An error occurred: {e}")
        print(traceback.format_exc())  

if __name__ == "__main__":
    main()
