import subprocess
import psycopg2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define database connection parameters
host = "localhost"
user = "postgres"
dbname = "gis"
password = "postgres"
shapefile_path = "C:/Users/rfrith/trevity/data/Vehicle_Ownership_and_Commuting_2019_Selected_Variables.shp"
temp_table_name = "temp_vehicle_ownership_and_commuting"
final_table_name = "vehicle_ownership_and_commuting"
ogr2ogr_path = r"C:\Program Files\GDAL\ogr2ogr.exe"  # Update this path to where ogr2ogr.exe is located

# Connect to the database
conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
cur = conn.cursor()

try:
    # Ensure PostGIS extension is enabled
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    conn.commit()

    # Drop the temporary and final tables if they exist
    cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
    cur.execute(f"DROP TABLE IF EXISTS {final_table_name};")
    conn.commit()

    # Create the temporary table with the correct precision for shape__are
    create_temp_table_sql = f"""
    CREATE TABLE {temp_table_name} (
        id SERIAL PRIMARY KEY,
        shape__are NUMERIC(30, 15),
        geom GEOMETRY(MultiPolygon, 4326)
        -- Add other columns here as needed
    );
    """
    logger.info(f"Creating temporary table with SQL: {create_temp_table_sql}")
    cur.execute(create_temp_table_sql)
    conn.commit()

    # Import the shapefile into the temporary table
    command = [
        ogr2ogr_path,
        "-f", "PostgreSQL",
        f"PG:host={host} user={user} dbname={dbname} password={password}",
        shapefile_path,
        "-nln", temp_table_name,
        "-nlt", "MULTIPOLYGON",
        "-lco", "GEOMETRY_NAME=geom",
        "-lco", "FID=id"
    ]
    logger.info(f"Running command: {' '.join(command)}")
    
    # Capture the output and error
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running ogr2ogr command: {result.stderr}")
        exit(1)
    else:
        logger.info(f"ogr2ogr output: {result.stdout}")

    # Inspect the temporary table to get the full schema
    cur.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{temp_table_name}';
    """)
    columns = cur.fetchall()
    logger.info(f"Columns in temporary table: {columns}")

    if not columns:
        logger.error("No columns found in temporary table. Exiting.")
        exit(1)

    # Create the final table with adjusted schema
    create_table_sql = f"CREATE TABLE {final_table_name} ("
    for column in columns:
        name, data_type = column
        if name == "shape__are":
            data_type = "numeric(30, 15)"  # Adjust precision for shape__are
        elif data_type == "USER-DEFINED":
            cur.execute(f"""
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_name = '{temp_table_name}' AND column_name = '{name}';
            """)
            udt_name = cur.fetchone()[0]
            if udt_name == "geometry":
                data_type = "geometry(MultiPolygon, 4326)"
        create_table_sql += f"{name} {data_type}, "
    create_table_sql = create_table_sql.rstrip(", ") + ");"
    logger.info(f"Creating final table with SQL: {create_table_sql}")
    cur.execute(create_table_sql)
    conn.commit()

    # Copy data from the temporary table to the final table
    copy_columns = ", ".join([col[0] for col in columns])
    logger.info(f"Copying data with columns: {copy_columns}")
    cur.execute(f"""
        INSERT INTO {final_table_name} ({copy_columns})
        SELECT {copy_columns} FROM {temp_table_name};
    """)
    conn.commit()

    # Drop the temporary table
    #cur.execute(f"DROP TABLE {temp_table_name};")
    #conn.commit()

    logger.info("Data import completed successfully.")
except subprocess.CalledProcessError as e:
    logger.error(f"Error running ogr2ogr command: {e}")
except psycopg2.Error as e:
    logger.error(f"Database error: {e}")
finally:
    # Close the database connection
    cur.close()
    conn.close()
