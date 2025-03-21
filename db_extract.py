

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseSchemaExtractor:
    def __init__(self, connection_string: str):
        try:
            logger.info(f"Initializing connection to database with connection string: {self._mask_password(connection_string)}")
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
            logger.info("Successfully initialized database connection")
        except Exception as e:
            logger.error(f"Connection Initialization Error: {e}")
            raise
    
    def _mask_password(self, conn_string: str) -> str:
        """Masks the password in the connection string for logging purposes"""
        if '@' in conn_string and ':' in conn_string:
            parts = conn_string.split('@')
            credentials = parts[0].split(':')
            if len(credentials) > 2:
                # Format like postgresql://user:password@host:port/dbname
                masked = f"{credentials[0]}:{credentials[1]}:****"
                for i in range(3, len(credentials)):
                    masked += f":{credentials[i]}"
                return f"{masked}@{parts[1]}"
        return conn_string.replace(conn_string, "****")
    
    def get_schema(self) -> str:
        try:
            logger.info("Starting schema extraction")
            inspector = inspect(self.engine)
            schema_info = []
            
            table_names = inspector.get_table_names()
            logger.info(f"Found {len(table_names)} tables")
            
            for table_name in table_names:
                columns = inspector.get_columns(table_name)
                logger.info(f"Extracting schema for table: {table_name} with {len(columns)} columns")
                
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
                    logger.warning(f"Error extracting foreign keys for table {table_name}: {fk_error}")
                    fk_details = [f"Error extracting foreign keys: {fk_error}"]
                
                table_schema = f"Table: {table_name}\n"
                table_schema += "\n".join(column_details)
                
                if fk_details:
                    table_schema += "\n\nForeign Keys:\n" + "\n".join(fk_details)
                
                schema_info.append(table_schema)
            
            full_schema = "\n\n".join(schema_info)
            logger.info(f"Schema extraction completed successfully. Schema length: {len(full_schema)}")
            return full_schema
        
        except Exception as e:
            logger.error(f"Schema Extraction Error: {e}")
            return f"Error extracting schema: {e}"

    def _clean_column_name(self, column_name: str) -> str:
        """Remove quotes, backticks, and table prefixes from column names."""
        # Remove table prefixes (table_name.column or "table_name"."column")
        if '.' in column_name:
            column_name = column_name.split('.')[-1]
            
        # Remove backticks, double quotes
        column_name = column_name.replace('`', '').replace('"', '')
        
        return column_name.strip()
