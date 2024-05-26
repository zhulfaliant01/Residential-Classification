import geopandas as gpd
import osmnx as osm
import warnings
import os

warnings.catch_warnings(action="ignore")

for kec in [
    "Cilincing",
    "Kelapa Gading",
    "Koja",
    "Penjaringan",
    "Pademangan",
    "Tanjung Priok",
]:
    graphs = osm.graph_from_place(f"{kec}, Jakarta Utara, Indonesia")
    output_dir = r"Data Collection\street\validation"
    file_path = os.path.join(output_dir, f"streets_{kec}_drive.geojson")

    try:
        streets = osm.graph_to_gdfs(
            graphs,
            nodes=False,
            edges=True,
            node_geometry=False,
            fill_edge_geometry=True,
        )
        streets = streets[~streets.highway.isna()].reset_index(drop=True)
        streets = streets[["geometry", "highway"]]
        for column in streets.columns:
            for index, row in streets.iterrows():
                if isinstance(row[column], list):
                    streets.loc[index, column] = ""
        streets.to_file(file_path, driver="GeoJSON")
    except Exception as e:
        print(f"Failed to export {e}")
