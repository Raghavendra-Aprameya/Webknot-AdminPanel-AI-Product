

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

        query_executor = DatabaseQueryExecutor(CONNECTION_STRING)

        for idx, query_info in enumerate(finance_queries):
            if not isinstance(query_info, dict):
                print(f"❌ Skipping invalid query data: {query_info}")
                continue

            use_case = query_info.get("use_case", "Unknown Use Case")
            query = query_info.get("query", "")
            user_input_columns = query_info.get("user_input_columns", [])

            print(f"\n💡 **Use Case:** {use_case}")
            print(f"📌 **SQL Query:**\n{query}")
            print(f"📝 **User Input Columns:** {', '.join(user_input_columns) if user_input_columns else 'None'}")

            user_inputs = {}
            if user_input_columns:
                for col in user_input_columns:
                    user_inputs[col] = input(f"🔹 Enter value for `{col}`: ")

            print("\n⚡ Executing query...")
            query_results = query_executor.execute_queries([query_info], user_inputs)

            for result in query_results:
                if "error" in result:
                    print(f"❌ **Error:** {result['error']}")
                else:
                    print(f"✅ **Query Executed Successfully!**")

                    # ✅ Display actual query results for SELECT statements
                    if isinstance(result["results"], list):
                        if result["results"]:
                            print("📌 **Query Results:**")
                            for row in result["results"]:
                                print(row)  # Print each row as a dictionary
                        else:
                            print("ℹ️ No records found.")
                    else:
                        print(f"ℹ️ {result['results']}")  # Print messages for non-SELECT queries

    except Exception as e:
        print(f"\n🚨 An error occurred: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()



