# utils/data_process.py
import pandas as pd
import geopandas as gpd
from shapely import wkt
import fiona
import os


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


def gdb_to_csv(gdb_file, layer_name, output_filename):  # Not done yet
    import os

    path = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Data Collection\Batas_wilayah"
    os.makedirs(path, exist_ok=True)

    for layer in layers:
        if layer == "ADMINISTRASI_AR_KECAMATAN":
            gdf = gpd.read_file(gdb_file, layer=layer)
            gdf.to_csv(os.path.join(path, "KECAMATAN.csv"))
        elif layer == "ADMINISTRASI_AR_KOTAKAB":
            gdf = gpd.read_file(gdb_file, layer=layer)
            gdf.to_csv(os.path.join(path, "KOTAKAB.csv"))
