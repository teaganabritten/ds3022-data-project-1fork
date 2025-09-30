import duckdb
import logging
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import calendar
import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analysis():

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=True)
        logger.info("Connected to DuckDB instance")
        print("Connected to DuckDB instance")

        # Step 1: Highest carbon-producing trips
        # Max Values for yellow cabs

        maxcarbonyellow = con.execute(f"""
                                                                            -- Most carbon-producing trip in yellow cabs
                                                        SELECT MAX(trip_co2_kgs) FROM taxidata
                                                                            WHERE vehicle_type = 'yellow_taxi';
                                                                                """)
        maxcarbonyellownumber = maxcarbonyellow.fetchone()[0]

        maxtripyellow = con.execute(f"""
                                        -- All info for highest trip in yellow cab
                                    SELECT * FROM taxidata
                                    WHERE vehicle_type = 'yellow_taxi'
                                    AND trip_co2_kgs = {maxcarbonyellownumber};
                    """)
        maxtripyellownumber = maxtripyellow.fetchall()
        print(f"Highest carbon-producing trip in yellow cabs: {maxtripyellownumber}")
        logger.info(f"Highest carbon-producing trip in yellow cabs: {maxtripyellownumber}")

        maxcarbongreen = con.execute(f"""
                                                                            -- Most carbon-producing trip in green cabs
                                                        SELECT MAX(trip_co2_kgs) FROM taxidata
                                                                            WHERE vehicle_type = 'green_taxi';
                                                                                """)
        maxcarbongreennumber = maxcarbongreen.fetchone()[0]
        # Process repeated for green cabs

        maxtripgreen = con.execute(f"""
                                        -- All info for highest trip in green cab
                                    SELECT * FROM taxidata
                                    WHERE vehicle_type = 'green_taxi'
                                    AND trip_co2_kgs = {maxcarbongreennumber};
                    """)
        maxtripgreennumber = maxtripgreen.fetchall()
        print(f"Highest carbon-producing trip in green cabs: {maxtripgreennumber}")
        logger.info(f"Highest carbon-producing trip in green cabs: {maxtripgreennumber}")

        # Step 2: highest and lowest hours for emissions
        # 

        byhour = con.execute(f"""
                -- Average CO₂ emissions per hour for yellow and green taxis
        WITH hourly_emissions AS (
            SELECT 
                vehicle_type,
                hour_of_day,
                AVG(trip_co2_kgs) AS avg_co2
            FROM taxidata
            WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
                GROUP BY vehicle_type, hour_of_day
                    )

                -- Find max and min average CO₂ per vehicle type
                SELECT * FROM hourly_emissions
                WHERE (vehicle_type, avg_co2) IN (
                    SELECT vehicle_type, MAX(avg_co2) FROM hourly_emissions GROUP BY vehicle_type
                    UNION
                    SELECT vehicle_type, MIN(avg_co2) FROM hourly_emissions GROUP BY vehicle_type
                )
                ORDER BY vehicle_type, avg_co2 DESC;        
                    """)

        results = byhour.fetchall()
        for row in results:
            vehicle_type, hour_of_day, avg_co2 = row
            print(f"{vehicle_type} - Hour {int(hour_of_day)}: {avg_co2:.2f} kg CO₂")
            logger.info(f"{vehicle_type} - Hour {int(hour_of_day)}: {avg_co2:.2f} kg CO₂")

        # Step 3: highest and lowest days of the week for emissions
        byday = con.execute(f"""
                -- highest and lowest CO₂ emissions days of the week for yellow and green taxis
                                   WITH daily_emissions AS (
            SELECT 
                vehicle_type,
                day_of_week,
                AVG(trip_co2_kgs) AS avg_co2
            FROM taxidata
            WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
                GROUP BY vehicle_type, day_of_week
                    )

                -- Find max and min average CO₂ per vehicle type
                SELECT * FROM daily_emissions
                WHERE (vehicle_type, avg_co2) IN (
                    SELECT vehicle_type, MAX(avg_co2) FROM daily_emissions GROUP BY vehicle_type
                    UNION
                    SELECT vehicle_type, MIN(avg_co2) FROM daily_emissions GROUP BY vehicle_type
                )
                ORDER BY vehicle_type, avg_co2 DESC;        
                    """)
        day_results = byday.fetchall()
        for row in day_results:
            vehicle_type, day_of_week, avg_co2 = row
            print(f"{vehicle_type} - Day {day_of_week}: {avg_co2:.2f} kg CO₂")
            logger.info(f"{vehicle_type} - Day {day_of_week}: {avg_co2:.2f} kg CO₂")
        # Step 4: highest and lowest weeks for emissions
        byweek = con.execute(f"""
                -- highest and lowest CO₂ emissions by week for yellow and green taxis
                                   WITH weekly_emissions AS (
            SELECT 
                vehicle_type,
                week_of_year,
                AVG(trip_co2_kgs) AS avg_co2
            FROM taxidata
            WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
                GROUP BY vehicle_type, week_of_year
                    )

                -- Find max and min average CO₂ per vehicle type
                SELECT * FROM weekly_emissions
                WHERE (vehicle_type, avg_co2) IN (
                    SELECT vehicle_type, MAX(avg_co2) FROM weekly_emissions GROUP BY vehicle_type
                    UNION
                    SELECT vehicle_type, MIN(avg_co2) FROM weekly_emissions GROUP BY vehicle_type
                )
                ORDER BY vehicle_type, avg_co2 DESC;        
                    """)
        week_results = byweek.fetchall()
        for row in week_results:
            vehicle_type, week_of_year, avg_co2 = row
            print(f"{vehicle_type} - Week {int(week_of_year)}: {avg_co2:.2f} kg CO₂")
            logger.info(f"{vehicle_type} - Week {int(week_of_year)}: {avg_co2:.2f} kg CO₂")

        # Step 5: highest and lowest months for emissions
        bymonth = con.execute(f"""
                -- highest and lowest CO₂ emissions by month for yellow and green taxis
                                   WITH monthly_emissions AS (
            SELECT 
                vehicle_type,
                month_of_year,
                AVG(trip_co2_kgs) AS avg_co2
            FROM taxidata
            WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
                GROUP BY vehicle_type, month_of_year
                    )

                -- Find max and min average CO₂ per vehicle type
                SELECT * FROM monthly_emissions
                WHERE (vehicle_type, avg_co2) IN (
                    SELECT vehicle_type, MAX(avg_co2) FROM monthly_emissions GROUP BY vehicle_type
                    UNION
                    SELECT vehicle_type, MIN(avg_co2) FROM monthly_emissions GROUP BY vehicle_type
                )
                ORDER BY vehicle_type, avg_co2 DESC;        
                    """)
        month_results = bymonth.fetchall()
        for row in month_results:
            vehicle_type, month_of_year, avg_co2 = row
            print(f"{vehicle_type} - Month {int(month_of_year)}: {avg_co2:.2f} kg CO₂")
            logger.info(f"{vehicle_type} - Month {int(month_of_year)}: {avg_co2:.2f} kg CO₂")
        
        # Final step: plot total CO2 per month across years for yellow and green taxis
        # Aggregate by year and month_of_year
        totals_query = con.execute(f"""
                SELECT vehicle_type, year, month_of_year, SUM(trip_co2_kgs) AS total_co2
                FROM taxidata
                WHERE vehicle_type IN ('yellow_taxi', 'green_taxi')
                GROUP BY vehicle_type, year, month_of_year
                ORDER BY year, month_of_year;
                """)
        totals = totals_query.fetchall()

        # Build mapping vehicle_type -> {(year,month): total}
        totals_map = defaultdict(lambda: defaultdict(float))
        ym_set = set()
        for vehicle_type, year_val, month_of_year, total_co2 in totals:
            y = int(year_val)
            m = int(month_of_year)
            ym_set.add((y,m))
            totals_map[vehicle_type][(y,m)] = float(total_co2)

        # Build ordered list of year-months covering the data (or 2015-2024)
        if ym_set:
            start = min(ym_set)
            end = max(ym_set)
            start_date = datetime.date(start[0], start[1], 1)
            end_date = datetime.date(end[0], end[1], 1)
            # create list of month starts from start_date to end_date inclusive
            months_dt = []
            cur = start_date
            while cur <= end_date:
                months_dt.append(cur)
                # advance one month
                if cur.month == 12:
                    cur = datetime.date(cur.year+1, 1, 1)
                else:
                    cur = datetime.date(cur.year, cur.month+1, 1)
        else:
            # fallback to full range 2015-2024
            months_dt = [datetime.date(y, m, 1) for y in range(2015,2025) for m in range(1,13)]

        # Build series values
        yellow_vals = [totals_map['yellow_taxi'].get((d.year, d.month), 0.0) for d in months_dt]
        green_vals = [totals_map['green_taxi'].get((d.year, d.month), 0.0) for d in months_dt]

        plt.figure(figsize=(12,5))
        plt.plot(months_dt, yellow_vals, marker='o', label='Yellow taxi')
        plt.plot(months_dt, green_vals, marker='o', label='Green taxi')
        plt.gca().xaxis.set_major_locator(mdates.YearLocator())
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.gca().xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1,4,7,10]))
        plt.xlabel('Month (year)')
        plt.ylabel('Total CO₂ (kg)')
        plt.title('Total CO₂ by Month (2015-2024) — Yellow vs Green taxis')
        plt.legend()
        plt.grid(alpha=0.3)
        plt.tight_layout()

        out_path = 'co2_by_month_timeseries.png'
        plt.savefig(out_path)
        print(f"Saved CO₂ by month timeseries plot to {out_path}")
        logger.info(f"Saved CO₂ by month timeseries plot to {out_path}")

    except Exception as e:
        logger.exception("Error running analysis")
        raise

    finally:
        try:
            if con is not None:
                con.close()
                logger.info("DuckDB connection closed")
        except Exception:
            logger.exception("Error closing DuckDB connection")

if __name__ == "__main__":
    analysis()




        

