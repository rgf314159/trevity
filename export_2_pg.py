import fiona
import zipfile
import os
import geopandas as gpd
import subprocess
from sqlalchemy import create_engine
import bokeh
from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.models import GeoJSONDataSource
import pandas as pd
from shapely import wkt

# Paths to the uploaded zip files
gdb_zip_path = 'C:/Users/rfrith/trevity/data/DOT_Index_State_5_3.gdb.zip'


# Extract the contents of the zip file
gdb_extract_path = 'C:/Users/rfrith/trevity/data/DOT_Index_State_5_3.gdb'

with zipfile.ZipFile(gdb_zip_path, 'r') as gdb_zip:
    gdb_zip.extractall(gdb_extract_path)



# List files in the extracted directory
gdb_files = os.listdir(gdb_extract_path)

layers = fiona.listlayers(gdb_extract_path)
print("Layers in the geodatabase:")
for layer in layers:
    print(layer)


from osgeo import gdal
print(gdal.VersionInfo())

print("bokeh version")
print(bokeh.__version__)

# Define paths and database connection details
gdb_path = 'C:/Users/rfrith/trevity/data/DOT_Index_State_5_3.gdb'
database = 'gis'
user = 'postgres'
password = 'postgres'
host = 'localhost'

# Convert each layer to PostgreSQL using ogr2ogr
for layer in layers:
    command = [
        'ogr2ogr',
        '-f', 'PostgreSQL',
        f"PG:dbname={database} user={user} password={password} host={host}",
        gdb_path,
        '-sql', f"SELECT * FROM {layer}",
        '-nln', layer
    ]
    #subprocess.run(command, check=True)

print("All layers have been imported into the PostGIS database.")

# Database connection
engine = create_engine('postgresql://postgres:postgres@localhost:5432/gis')

# Query to get table columns
query = """
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'dot_index_state_5_3'
"""

# Execute the query and fetch the columns
columns_df = pd.read_sql(query, engine)

# Print the columns
print(columns_df)



query = """
SELECT *, ST_AsText(ST_Force2D(ST_SetSRID(ST_GeomFromEWKB(wkb_geometry::bytea), 4326))) AS geom
FROM dot_index_state_5_3
WHERE stbbr = 'AL'
"""
# Read the data into a DataFrame without specifying geom_col
df = pd.read_sql(query, engine)


# Convert the WKT geometry to shapely geometry objects
df['geom'] = df['geom'].apply(wkt.loads)

# Create a GeoDataFrame from the DataFrame
gdf = gpd.GeoDataFrame(df, geometry='geom')

# Drop non-serializable columns (such as memoryview columns if they exist)
# Identify columns to drop (if any)
non_serializable_columns = [col for col in gdf.columns if isinstance(gdf[col].iloc[0], memoryview)]
gdf.drop(columns=non_serializable_columns, inplace=True)

# Convert GeoDataFrame to GeoJSON
geojson_data = gdf.to_json()
geosource = GeoJSONDataSource(geojson=geojson_data)

# Create Bokeh plot
p = figure(title="Geospatial Data Visualization - AL", 
           width=800, 
           height=600)

p.patches('xs', 'ys', source=geosource, fill_alpha=0.5, line_color="black", line_width=0.5)

# Save and show the plot
output_file("geospatial_plot_AL.html")
show(p)
