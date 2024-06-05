import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
engine = create_engine('postgresql://postgres:postgres@localhost:5432/gis')
connection = engine.connect()

query = text("""
    SELECT v.tract_name
           , v.fid
           , v.pct_noveh
           , v.pcomcarpl
           , d.lctn
           , ST_AsText(ST_SetSRID(ST_GeomFromWKB(d.wkb_geometry::bytea), 4326)) AS geom
           , d.stt
           , d.cnty
           , d.avgcmm
    FROM public.vehicle_ownership_and_commuting v
    JOIN public.dot_index_state_5_3 d
    ON d.lctn LIKE '%' || v.tract_name || '%'
    WHERE d.stbbr = 'AL'
    limit 10
""")

try:
    result = connection.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print(df.head())
except Exception as e:
    print(f"Error executing full query: {e}")
finally:
    connection.close()
