import geopandas as gpd
import osmnx as osm
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import os

warnings.catch_warnings(action="ignore")

graphs = osm.graph_from_place("Jakarta Selatan, Indonesia")
output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Data Collection\street"
file_path = os.path.join(output_dir, f"streets_jaksel_drive.geojson")

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
