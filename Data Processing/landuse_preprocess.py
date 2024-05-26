import geopandas as gpd
import pandas as pd
import glob
import os
import json

# Training or Test datasets?
type = "training"

# Load configuration
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

folder_in = config.get(f"landuse_{type}_paths")["input"]
folder_out = config.get(f"landuse_{type}_paths")["output"]

os.makedirs(folder_out, exist_ok=True)

crs = config.get("crs")


def map_label(gdf):
    for _, row in gdf.iterrows():
        if row["D_SUB_PENG"] == "HUNIAN" and row["D_KEGIATAN"] not in [
            "RUMAH SUSUN",
            "RUMAH SUSUN UMUM",
        ]:
            gdf.loc[_, "label"] = 1
        else:
            gdf.loc[_, "label"] = 0
    gdf = gdf.dissolve("label", as_index=False)
    return gdf[["label", "geometry"]]


def create_gdf(files):
    gdf_list = []
    for file in files:
        print(file)
        df = gpd.read_file(file)
        gdf_list.append(df)

    gdf = gpd.GeoDataFrame(pd.concat(gdf_list))
    return gdf


# Change to WKT format later (CSV)
files = glob.glob(os.path.join(folder_in, "*.shp"))
gdf = create_gdf(files)
gdf.reset_index(inplace=True)
gdf = map_label(gdf)

if gdf.crs != crs:
    gdf = gdf.set_crs(crs, allow_override=True)  # type: ignore

path_file = os.path.join(folder_out, f"{type}_landuse.csv")
gdf.to_csv("test.csv")
