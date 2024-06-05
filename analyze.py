import fiona
import zipfile
import os
import geopandas as gpd

# Paths to the uploaded zip files
gdb_zip_path = 'C:/Users/rfrith/trevity/data/DOT_Index_State_5_3.gdb.zip'
csv_zip_path = 'C:/Users/rfrith/trevity/data/Vehicle_Ownership_and_Commuting_2019_Selected_Variables.zip'

# Extract the contents of the zip file
gdb_extract_path = 'C:/Users/rfrith/trevity/data/DOT_Index_State_5_3.gdb'
csv_extract_path = 'C:/Users/rfrith/trevity/data/Vehicle_Ownership_and_Commuting_2019_Selected_Variables'

with zipfile.ZipFile(gdb_zip_path, 'r') as gdb_zip:
    gdb_zip.extractall(gdb_extract_path)

with zipfile.ZipFile(csv_zip_path, 'r') as csv_zip:
    csv_zip.extractall(csv_extract_path)

# List files in the extracted directory
gdb_files = os.listdir(gdb_extract_path)
csv_files = os.listdir(csv_extract_path)

print("Extracted GDB files:", gdb_files)
print("Extracted CSV files:", csv_files)

# Path to the geodatabase
gdb_path = os.path.join(gdb_extract_path, 'DOT_Index_State_5_3.gdb')  # Adjust the filename if necessary

# List the layers in the geodatabase
layers = fiona.listlayers(gdb_path)
print("Layers in the geodatabase:")
for layer in layers:
    print(layer)


layer_name = 'DOT_Index_State_5_3'

# Read the geodatabase layer
gdf = gpd.read_file(gdb_path, layer=layer_name)

# Read the CSV file
#csv_df = gpd.read_file(csv_path)

# Print attributes of the datasets
#print("Attributes of the Geodatabase layer:")
#print(gdf.columns)

#print("\nAttributes of the CSV file:")
#print(csv_df.columns)

# Read the specified layer into a GeoDataFrame
gdf = gpd.read_file(gdb_path, layer=layer_name)

# Convert the GeoDataFrame to GeoJSON
geojson_data = gdf.to_json()

# Create a GeoJSONDataSource
geosource = GeoJSONDataSource(geojson=geojson_data)

# Create a Bokeh plot
p = figure(title="Geospatial Data Visualization - Layer 'DOT_Index_State_5_3'", 
           plot_width=800, 
           plot_height=600,
           tools="pan,wheel_zoom,box_zoom,reset")

# Add patches to the plot
p.patches('xs', 'ys', source=geosource, fill_alpha=0.5, line_color="black", line_width=0.5)

# Display the plot
output_file("C:/Users/rfrith/trevity/data/geospatial_plot_layer_x.html")
show(p)




