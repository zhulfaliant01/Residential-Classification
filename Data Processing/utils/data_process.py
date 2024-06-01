# utils/data_process.py
import pandas as pd
import geopandas as gpd
from shapely import wkt
import fiona
import json
import gc
import numpy as np


def read_csv_to_wkt(file, geom_column="geometry", index_col=None):
    """
    Reads a CSV file and converts its geometry column to Well-Known Text (WKT) format.

    Parameters:
    - file (str): The path to the CSV file.
    - geom_column (str, optional): The name of the geometry column in the CSV file. Defaults to "geometry".
    - index_col (str or list, optional): The name or list of names of the index column(s) in the CSV file. If not provided, it defaults to no index column.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame object with the geometry column converted to WKT format.

    Raises:
    - ValueError: If the CSV file does not contain a geometry column or if the geometry column contains null values.

    Example:
    ```
    df = read_csv_to_wkt('path/to/your/csv_file.csv')
    ```
    """
    print("Reading CSV file...")

    if index_col is None:
        data = pd.read_csv(file)
    else:
        data = pd.read_csv(file, index_col=index_col)

    print(f"Data loaded: {data.shape[0]} rows and {data.shape[1]} columns")

    print("Applying WKT loads...")
    null_data = data.geometry.isna().sum()
    if null_data:
        print(f"There is {null_data} null geometry")
    data = data.dropna(subset=[geom_column])

    data[geom_column] = data[geom_column].astype(str)
    data[geom_column] = data[geom_column].apply(wkt.loads)
    print(f"WKT conversion done: {data[geom_column].notnull().sum()} geometries loaded")

    print("Converting to GeoDataFrame...")
    data = gpd.GeoDataFrame(data, geometry=geom_column)  # type: ignore
    print("Conversion to GeoDataFrame done.")

    return data


def del_gc(var):
    del var
    gc.collect()


def mm_std_character(gdf, sw, value: str, unique_id: str):
    data = gdf.copy()
    data = data.set_index(unique_id)[value]
    std = []
    for index in data.index:
        if index in sw.neighbors:
            neighbors = [index]
            neighbors += sw.neighbors[index]

            values_list = gdf.loc[neighbors]
            std.append(np.std(values_list))

    std = pd.Series(std, index=gdf.index)
    return std


def mm_total_area(gdf, sw, unique_id):
    data = gdf.copy()
    data = data.set_index(unique_id)["b_area"]
    total_area = []
    for index in data.index:
        if index in sw.neighbors:
            neighbors = [index]
            neighbors += sw.neighbors[index]
            values_list = gdf.loc[neighbors]
            total_area.append(np.sum(values_list))
    total_area = pd.Series(total_area, index=gdf.index)
    return total_area


def list_gdb_layers(gdb_file):
    layers = fiona.listlayers(gdb_file)
    return layers


def check_and_set_crs(gdf: gpd.GeoDataFrame, crs):
    if gdf.crs != crs:
        gdf.set_crs(crs, allow_override=True)
    return gdf


def gdb_to_csv(gdb_file, layer_name, output_filename):
    pass
