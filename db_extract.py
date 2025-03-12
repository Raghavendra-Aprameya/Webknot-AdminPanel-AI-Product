from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect

class DatabaseSchemaExtractor:
    def __init__(self, connection_string: str):
        try:
            self.engine = create_engine(
                connection_string, 
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    'connect_timeout': 10
                }
            )
            
            self.Session = sessionmaker(bind=self.engine)
            
            self.metadata = MetaData()
        except Exception as e:
            print(f"Connection Initialization Error: {e}")
            raise
    
    def get_schema(self) -> str:
        try:
            inspector = inspect(self.engine)
            schema_info = []
            
            table_names = inspector.get_table_names()
            
            for table_name in table_names:
                columns = inspector.get_columns(table_name)
                
                column_details = []
                for col in columns:
                    col_name = col.get('name', 'Unknown')
                    col_type = str(col.get('type', 'Unknown'))
                    
                    nullable = col.get('nullable', True)
                    nullable_str = "NULL" if nullable else "NOT NULL"
                    
                    column_details.append(
                        f"{col_name} ({col_type}) {nullable_str}"
                    )
                
                try:
                    foreign_keys = inspector.get_foreign_keys(table_name)
                    fk_details = []
                    for fk in foreign_keys:
                        fk_details.append(
                            f"FK: {fk.get('name', 'Unknown')} - " +
                            f"{fk.get('constrained_columns', 'Unknown')} → " +
                            f"{fk.get('referred_table', 'Unknown')}"
                        )
                except Exception as fk_error:
                    fk_details = [f"Error extracting foreign keys: {fk_error}"]
                
                table_schema = f"Table: {table_name}\n"
                table_schema += "\n".join(column_details)
                
                if fk_details:
                    table_schema += "\n\nForeign Keys:\n" + "\n".join(fk_details)
                
                schema_info.append(table_schema)
            
            return "\n\n".join(schema_info)
        
        except Exception as e:
            print(f"Schema Extraction Error: {e}")
            return f"Error extracting schema: {e}"