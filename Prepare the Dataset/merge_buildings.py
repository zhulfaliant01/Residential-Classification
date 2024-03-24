# Untuk merge data bangunan
import geopandas as gpd
import glob
import os
import pandas as pd


folder_path = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\building"
os.chdir(folder_path)
files = glob.glob("*.geojson")
gdf_list = []
for file in files:
    df = gpd.read_file(file)
    gdf_list.append(df)

gdf = gpd.GeoDataFrame(pd.concat(gdf_list))

output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\building"
path_file = os.path.join(output_dir, "building_jaksel.geojson")
gdf.to_file(path_file, driver="GeoJSON")
