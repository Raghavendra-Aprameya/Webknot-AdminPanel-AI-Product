import traceback
from pprint import pprint
from config import (
    CONNECTION_STRING, 
    GOOGLE_API_KEY, 
    LLM_MODEL, 
    DEFAULT_NUM_QUERIES
)
from dotenv import load_dotenv
from db_extract import DatabaseSchemaExtractor
from query_generator import FinanceQueryGenerator
from query_exec import DatabaseQueryExecutor


def main():
    try:
        
        schema_extractor = DatabaseSchemaExtractor(CONNECTION_STRING)
        schema = schema_extractor.get_schema()
        print("Schema extracted successfully")
        
        query_generator = FinanceQueryGenerator(
            schema=schema,
            api_key=GOOGLE_API_KEY,
            model=LLM_MODEL
        )

        finance_queries = query_generator.get_queries_for_executor(
            role="Finance Manager", domain="finance")
        
        print(f"Generated {len(finance_queries)} finance queries")
        
        query_executor = DatabaseQueryExecutor(CONNECTION_STRING)
        query_results = query_executor.execute_queries(finance_queries)
        
        print("=== Finance Manager Query Analysis ===")
        for i, result in enumerate(query_results, 1):
            print(f"\n--- Query {i} ---")
            print(f"SQL Query:\n{result['query']}")
            print(f"\nExplanation:\n{result.get('explanation', 'No explanation provided')}")

            if 'results' in result:
                    print("\n--- Results ---")
                    if result['results'] and not 'error' in result['results'][0]:
                        headers = result['results'][0].keys()
                        header_row = ' | '.join(str(h).ljust(20) for h in headers)
                        print(header_row)
                        print('-' * len(header_row))
    
                    for row in result['results'][:10]:  
                        if 'error' in row:
                            print(f"Error processing row: {row['error']}")
                            if 'row_data' in row:
                                print(f"Raw data: {row['row_data']}")
                        else:
                            print(' | '.join(str(v).ljust(20) for v in row.values()))
    
                    if len(result['results']) > 10:
                        print(f"... and {len(result['results']) - 10} more rows")
            elif 'error' in result:
                    print(f"\n--- Error ---\n{result['error']}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())  

if __name__ == "__main__":
    main()