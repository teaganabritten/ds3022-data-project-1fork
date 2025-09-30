import duckdb
import logging

'''
My Transformations are completed using DBT. The SQL for that is located in dbt/models/
'''

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

def transform():
    con = None
    
    try: # Drops yellow and green separate tables after combined table is made
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance for transformation")
        print("Connected to DuckDB instance for transformation")

        con.execute(f"""
            DROP TABLE IF EXISTS yellow_tripdata;
            DROP TABLE IF EXISTS green_tripdata;
            """)
        logger.info("Dropped yellow and green tables to prepare for dbt transformations")
        print("Dropped yellow and green tables to prepare for dbt transformations")

    except Exception as e:
        logger.exception("Error during transformation process")
        print(f"Error during transformation process: {e}")
    finally:
        if con:
            try:
                con.close()
                logger.info("DuckDB connection closed")
                print("DuckDB connection closed")
            except Exception:
                logger.exception("Error closing DuckDB connection")
if __name__ == "__main__":
    transform()