import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_data():
    con = None
    
    try:
       con = duckdb.connect(database='emissions.duckdb', read_only=False)
       logger.info("Connected to DuckDB instance for cleaning")
       print("Connected to DuckDB instance for cleaning")
       # removes trips without passengers, with no distance or distance over 100 miles
       con.execute(f"""
              CREATE OR REPLACE TABLE green_tripdata_clean AS
                SELECT DISTINCT * FROM green_tripdata WHERE 
                    pickup_datetime IS NOT NULL
                    AND dropoff_datetime IS NOT NULL
                    AND passenger_count > 0
                    AND trip_distance > 0
                    AND trip_distance <= 100
                    ;
            DROP TABLE green_tripdata;
            ALTER TABLE green_tripdata_clean RENAME TO green_tripdata;
                """)
       # Calculate trip duration in seconds and filter out invalid durations
       con.execute(f"""
            CREATE OR REPLACE TABLE green_tripdata_modified AS
                SELECT * FROM green_tripdata
                   ;
               ALTER TABLE green_tripdata_modified DROP COLUMN IF EXISTS trip_duration;
            ALTER TABLE green_tripdata_modified ADD COLUMN trip_duration DOUBLE
                   ;
            UPDATE green_tripdata_modified
            SET trip_duration = EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime));
            
            DROP TABLE green_tripdata;
            ALTER TABLE green_tripdata_modified RENAME TO green_tripdata;
            """)
       # removes lines with duration of zero or over 86400 seconds (24 hours)
       con.execute(f"""
            CREATE OR REPLACE TABLE green_tripdata_final AS
            SELECT * FROM green_tripdata WHERE
                trip_duration > 0
                AND trip_duration <= 86400
                ;
            DROP TABLE green_tripdata;
            ALTER TABLE green_tripdata_final RENAME TO green_tripdata;
            """)
       # Series of chesk to ensure cleaning was successful
       passengercheck = con.execute(f"""
            -- Count records with 0 passengers
            SELECT COUNT(*) FROM green_tripdata WHERE passenger_count = 0;
            """)
       passengernumber = passengercheck.fetchone()[0]
       print(f"Number of green records with 0 passengers: {passengernumber}")
       logger.info(f"Number of green records with 0 passengers: {passengernumber}")

       distancecheck = con.execute(f"""
            -- Count records with 0 distance or over 100 miles
            SELECT COUNT(*) FROM green_tripdata WHERE trip_distance = 0 OR trip_distance > 100;
            """)
       distancenumber = distancecheck.fetchone()[0]
       print(f"Number of green records with 0 distance or over 100 miles: {distancenumber}")
       logger.info(f"Number of green records with 0 distance or over 100 miles: {distancenumber}")

       timecheck = con.execute(f"""
            -- Count records with 0 or over 86400 seconds trip duration
            SELECT COUNT(*) FROM green_tripdata WHERE trip_duration = 0 OR trip_duration > 86400;
            """)
       timenumber = timecheck.fetchone()[0]
       print(f"Number of green records with 0 or over 86400 seconds trip duration: {timenumber}")
       logger.info(f"Number of green records with 0 or over 86400 seconds trip duration: {timenumber}") 
       
       print("Green cleaning complete")
       logger.info("Green cleaning complete")

       # Repeat the same cleaning process for yellow trip data

       con.execute(f"""
              CREATE OR REPLACE TABLE yellow_tripdata_clean AS
                SELECT DISTINCT * FROM yellow_tripdata WHERE 
                    pickup_datetime IS NOT NULL
                    AND dropoff_datetime IS NOT NULL
                    AND passenger_count > 0
                    AND trip_distance > 0
                    AND trip_distance <= 100
                    ;
            DROP TABLE yellow_tripdata;
            ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata;
                """)
       
       con.execute(f"""
            CREATE OR REPLACE TABLE yellow_tripdata_modified AS
                SELECT * FROM yellow_tripdata
                   ;
               ALTER TABLE yellow_tripdata_modified DROP COLUMN IF EXISTS trip_duration;
            ALTER TABLE yellow_tripdata_modified ADD COLUMN trip_duration DOUBLE
                ;
            UPDATE yellow_tripdata_modified
            SET trip_duration = EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime));
            
            DROP TABLE yellow_tripdata;
            ALTER TABLE yellow_tripdata_modified RENAME TO yellow_tripdata;
            """)
       
       con.execute(f"""
            CREATE OR REPLACE TABLE yellow_tripdata_final AS
            SELECT * FROM yellow_tripdata WHERE
                trip_duration > 0
                AND trip_duration <= 86400
                ;
            DROP TABLE yellow_tripdata;
            ALTER TABLE yellow_tripdata_final RENAME TO yellow_tripdata;
            """)
       passengercheck = con.execute(f"""
            -- Count records with 0 passengers
            SELECT COUNT(*) FROM yellow_tripdata WHERE passenger_count = 0;
            """)
       passengernumber = passengercheck.fetchone()[0]
       print(f"Number of yellow records with 0 passengers: {passengernumber}")
       logger.info(f"Number of yellow records with 0 passengers: {passengernumber}")

       distancecheck = con.execute(f"""
            -- Count records with 0 distance or over 100 miles
            SELECT COUNT(*) FROM yellow_tripdata WHERE trip_distance = 0 OR trip_distance > 100;
            """)
       distancenumber = distancecheck.fetchone()[0]
       print(f"Number of yellow records with 0 distance or over 100 miles: {distancenumber}")
       logger.info(f"Number of yellow records with 0 distance or over 100 miles: {distancenumber}")

       timecheck = con.execute(f"""
            -- Count records with 0 or over 86400 seconds trip duration
            SELECT COUNT(*) FROM yellow_tripdata WHERE trip_duration = 0 OR trip_duration > 86400;
            """)
       timenumber = timecheck.fetchone()[0]
       print(f"Number of yellow records with 0 or over 86400 seconds trip duration: {timenumber}")
       logger.info(f"Number of yellow records with 0 or over 86400 seconds trip duration: {timenumber}")
       
       print("Yellow cleaning complete")
       logger.info("Yellow cleaning complete")

    except Exception as e:
         logger.error(f"Error during cleaning: {e}")
         print(f"Error during cleaning: {e}")

if __name__ == "__main__":
    clean_data()

        

