# utils/data_process.py
import pandas as pd
import geopandas as gpd
from shapely import wkt
import fiona
import json

with open(r"config.json", "r") as config_file:
    config = json.load(config_file)
crs = config.get("crs")


def read_csv_to_wkt(file, geom_column="geometry"):
    print("Reading CSV file...")
    data = pd.read_csv(file, index_col=0)
    print(f"Data loaded: {data.shape[0]} rows and {data.shape[1]} columns")

    print("Applying WKT loads...")
    data[geom_column] = data[geom_column].apply(wkt.loads)
    print(f"WKT conversion done: {data[geom_column].notnull().sum()} geometries loaded")

    print("Converting to GeoDataFrame...")
    data = gpd.GeoDataFrame(data, geometry=geom_column)  # type: ignore
    print("Conversion to GeoDataFrame done.")

    return data


def list_gdb_layers(gdb_file):
    layers = fiona.listlayers(gdb_file)
    return layers


def check_and_set_crs(gdf: gpd.GeoDataFrame):
    if gdf.crs != crs:
        gdf.set_crs(crs, allow_override=True)
    return gdf


def gdb_to_csv(gdb_file, layer_name, output_filename):
    pass
