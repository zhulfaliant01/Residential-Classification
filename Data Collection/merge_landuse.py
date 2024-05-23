import geopandas as gpd
import pandas as pd
import glob
import os

folder_path = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Data Collection\landuse\validation"
os.chdir(folder_path)
files = glob.glob("*.shp")
gdf_list = []
for file in files:
    print(file)
    df = gpd.read_file(file)
    gdf_list.append(df)

gdf = gpd.GeoDataFrame(pd.concat(gdf_list))

path_file = os.path.join(folder_path, "landuse_jakut.geojson")
gdf.to_file(path_file, driver="GeoJSON")
