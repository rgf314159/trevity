from flask import Flask, render_template, request, jsonify
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from bokeh.embed import components
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar, HoverTool, TapTool, CustomJS
from bokeh.plotting import figure
from bokeh.transform import linear_cmap
from bokeh.palettes import RdYlGn
from shapely import wkt
import jenkspy
import numpy as np
from flask_paginate import Pagination, get_page_parameter
import logging

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def index():
    # Database connection
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/gis')

    # Read data from PostGIS with a filter and cast geometry column to WKT
    query = """
    SELECT *, ST_AsText(ST_SetSRID(ST_GeomFromWKB(wkb_geometry::bytea), 4326)) AS geom
    FROM dot_index_state_5_3
    WHERE stbbr = 'AL' and avgcmm is not NULL
    """

    # Read the data into a Pandas DataFrame
    df = pd.read_sql(query, engine)

    # Convert the WKT geometry to shapely geometry objects
    df['geom'] = df['geom'].apply(wkt.loads)
    print(df.columns)
    # Create a GeoDataFrame from the DataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geom')

    # Drop non-serializable columns (such as memoryview columns if they exist)
    non_serializable_columns = [col for col in gdf.columns if isinstance(gdf[col].iloc[0], memoryview)]
    gdf.drop(columns=non_serializable_columns, inplace=True)

    # Handle missing or infinite values in avgcmm
    gdf = gdf.replace([np.inf, -np.inf], np.nan).dropna(subset=['avgcmm'])

    # Ensure all values in avgcmm are finite
    gdf = gdf[np.isfinite(gdf['avgcmm'])]

    # Calculate Jenks natural breaks
    try:
        jenks_breaks = jenkspy.JenksNaturalBreaks(n_classes=10)
        jenks_breaks.fit(gdf['avgcmm'])
        breaks = jenks_breaks.breaks_
    except Exception as e:
        print(f"Error calculating Jenks natural breaks: {e}")
        return "Error calculating Jenks natural breaks"

    # Create a color mapper based on Jenks breaks
    color_mapper = LinearColorMapper(palette=RdYlGn[10], low=min(gdf['avgcmm']), high=max(gdf['avgcmm']))
    gdf['colors'] = pd.cut(gdf['avgcmm'], bins=breaks, labels=RdYlGn[10], include_lowest=True)

    # Convert GeoDataFrame to GeoJSON
    geojson_data = gdf.to_json()
    geosource = GeoJSONDataSource(geojson=geojson_data)

    # Create Bokeh plot
    p = figure(title="Geospatial Data Visualization - AL (Classified by avgcmm)", width=800, height=600)

    # Add patches for the geospatial data with coloring based on Jenks breaks
    p.patches('xs', 'ys', source=geosource, fill_color={'field': 'avgcmm', 'transform': color_mapper},
              fill_alpha=0.7, line_color="black", line_width=0.5)

    # Add a hover tool
    hover = HoverTool()
    hover.tooltips = [("Location", "@lctn")]
    p.add_tools(hover)

    # Add a tap tool with SweetAlert for popups
    tap = TapTool(callback=CustomJS(args=dict(source=geosource), code="""
        var indices = cb_data.source.selected.indices;
        var data = source.data;
        var location = data['lctn'][indices[0]];
        
        fetch(`/get_popup_data?lctn=${encodeURIComponent(location)}`)
            .then(response => response.json())
            .then(data => {
                Swal.fire({
                    title: 'Location Information',
                    text: `Location: ${location}\nPcomcarpl: ${data.pcomcarpl}`,
                    icon: 'info',
                    confirmButtonText: 'OK'
                });
            })
            .catch(error => {
                console.error('Error:', error);
                Swal.fire({
                    title: 'Error',
                    text: 'Failed to fetch data',
                    icon: 'error',
                    confirmButtonText: 'OK'
                });
            });
    """))
    p.add_tools(tap)

    # Add a color bar
    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=12, width=8, location=(0,0))
    p.add_layout(color_bar, 'right')

    # Embed plot into HTML via Flask Render
    script, div = components(p)

    # Pagination settings
    page = request.args.get(get_page_parameter(), type=int, default=1)
    per_page = 10
    total = len(df)
    df_paginated = df.iloc[(page - 1) * per_page: page * per_page]
    pagination = Pagination(page=page, total=total, per_page=per_page, css_framework='bootstrap4')

    # Fields to display in grid view
    fields = df_paginated[['stt', 'cnty', 'lctn', 'avgcmm']]

    return render_template("index.html", script=script, div=div, fields=fields.to_dict(orient='records'), pagination=pagination)

@app.route('/get_popup_data')
def get_popup_data():
    lctn = request.args.get('lctn')
    if not lctn:
        logger.error("No location provided")
        return jsonify({"error": "No location provided"}), 400

    # Sanitize lctn by removing all text including and after the first comma
    lctn_sanitized = lctn.split(',')[0]
    logger.info(f"Sanitized lctn: {lctn_sanitized}")

    # Database connection
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/gis')
    query = text("""
        SELECT pcomcarpl
        FROM public.vehicle_ownership_and_commuting
        WHERE tract_name LIKE :lctn
        LIMIT 1
    """)
    lctn_param = f"%{lctn_sanitized}%"
    logger.info(f"Executing query: {query} with parameter: {lctn_param}")
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"lctn": lctn_param}).fetchone()
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return jsonify({"error": "Database query failed", "details": str(e)}), 500

    if result:
        return jsonify({"pcomcarpl": result.pcomcarpl})
    else:
        return jsonify({"pcomcarpl": "No data available"})

if __name__ == '__main__':
    app.run(debug=True)
