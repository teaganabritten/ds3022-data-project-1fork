import duckdb
import os
import logging
import time


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

years = range(2015, 2025)
months = [f"{m:02d}" for m in range(1, 13)]

def load_parquet_files():
    i = 0
    j = 0

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        print("Connected to DuckDB instance")

        con.execute(f"""
            DROP TABLE IF EXISTS green_tripdata;
        """)
        logger.info("Dropped green table if exists")
        print("Dropped green table if exists")

        for year in years: # nested loop to loop through years and months (green cabs)
            for month in months:
                if i == 0:
                    con.execute(f"""
                        CREATE TABLE green_tripdata AS
                            SELECT lpep_pickup_datetime AS pickup_datetime, lpep_dropoff_datetime AS dropoff_datetime, passenger_count, trip_distance FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet')
                            ;
                        """)
                    time.sleep(20) # 20 second delay after each file
                    i+=1
                else:
                    con.execute(f"""
                        INSERT INTO green_tripdata
                            SELECT lpep_pickup_datetime AS pickup_datetime, lpep_dropoff_datetime AS dropoff_datetime, passenger_count, trip_distance FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet')
                            ;
                        """)
                    time.sleep(20)
                    i+=1
            logger.info(f"Loaded green data for {year}")
            print(f"Loaded green data for {year}")               
        
        greencount = con.execute(f""" 
            -- Count records
            SELECT COUNT(*) FROM green_tripdata;
        """) # selecting the count of green records for verification
        greennumber = greencount.fetchone()[0]
        print(f"Number of records in green table: {greennumber}")
        logger.info(f"Number of records in green table: {greennumber}")

        con.execute(f"""
            DROP TABLE IF EXISTS yellow_tripdata;
        """)
        logger.info("Dropped yellow table if exists")
        
        for year in years: # nested loops to loop through years and months of files (yellow cabs)
            for month in months:
                if j == 0:
                    con.execute(f"""
                        CREATE TABLE yellow_tripdata AS
                            SELECT tpep_pickup_datetime AS pickup_datetime, tpep_dropoff_datetime AS dropoff_datetime, passenger_count, trip_distance FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')
                            ;
                        """)
                    time.sleep(20) # 20 second delay after each file
                    j+=1
                else:
                    con.execute(f"""
                        INSERT INTO yellow_tripdata
                            SELECT tpep_pickup_datetime AS pickup_datetime, tpep_dropoff_datetime AS dropoff_datetime, passenger_count, trip_distance FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')
                            ;
                        """)
                    time.sleep(20)
                    j+=1
            logger.info(f"Loaded yellow data for {year}")
            print(f"Loaded yellow data for {year}")              
        
        yellowcount = con.execute(f"""
            -- Count record
            SELECT COUNT(*) FROM yellow_tripdata;
        """) # verifies number of yellow records
        yellownumber = yellowcount.fetchone()[0]

        print(f"Number of records in yellow table: {yellownumber}")
        logger.info(f"Number of records in yellow table: {yellownumber}")

        con.execute(f"""
            DROP TABLE IF EXISTS co2data;
        """)

        con.execute(f"""
                    CREATE TABLE co2data AS
                    SELECT * FROM read_csv('data/vehicle_emissions.csv');
                    """)
        logger.info("Created co2data table from CSV") # creates co2data table from local CSV file
        print("Created co2data table from CSV")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()