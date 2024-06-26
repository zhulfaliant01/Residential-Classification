import geopandas as gpd
import pandas as pd
import os
import logging
import json
import glob
import re
from utils import read_csv_to_wkt

# del os.environ["PROJ_LIB"]  # Ada conflict antara PROJ dari venv dengan PROJ dari PostgreSQL

# Set logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Set dataset type: "training" or "test"
dataset_type = "test"

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
folder_in = config.get(f"label_{dataset_type}_paths")["input"]
folder_out = config.get(f"label_{dataset_type}_paths")["output"]
landuse = config.get(f"label_{dataset_type}_paths")["landuse"]
os.makedirs(folder_out, exist_ok=True)

crs = config.get("crs")


def add_label(gdf, label, name):
    logging.info("Add label start!")

    intersections = gdf.overlay(label, how="intersection")
    logging.info(f"{name}: Intersections done...")

    df = intersections[["bID_kec", "label", "geometry"]]
    df = df.dissolve(["bID_kec", "label"])
    logging.info(f"{name}: Dissolve done...")

    df.reset_index(inplace=True)
    df["inter_area"] = df.geometry.area

    result = df.groupby("bID_kec", group_keys=False).apply(
        lambda x: x.loc[x["inter_area"].idxmax()], include_groups=True
    )

    result = result.drop(columns="bID_kec").reset_index()

    final_gdf = gdf.merge(
        result.drop(columns=["inter_area", "geometry"]),
        left_on="bID_kec",
        right_on="bID_kec",
    )

    final_gdf = gpd.GeoDataFrame(final_gdf)

    return final_gdf


files = glob.glob(os.path.join(folder_in, "2_*_building.csv"))
landuse_gdf = gpd.read_file(r"Dataset\2_landuse_clean\test\test_landuse_2.shp")
landuse_gdf = landuse_gdf.explode(index_parts=True)

if landuse_gdf.crs != crs:
    landuse_gdf = landuse_gdf.set_crs(crs, allow_override=True)  # type: ignore

for file in files:
    kec = re.search(r"\\2_([\w ]*)_building.csv", file).group(1)  # type: ignore #typeig
    logging.info(f"{kec} start...")
    building = read_csv_to_wkt(file)

    if building.crs != crs:
        building = building.set_crs(crs, allow_override=True)  # type: ignore

    xmin, ymin, xmax, ymax = building.total_bounds
    label = landuse_gdf.cx[xmin:xmax, ymin:ymax]

    building.geometry = building.geometry.buffer(0)
    building = building[building.geometry.is_valid]

    try:
        building.drop(columns="label", inplace=True, axis=1)
    except:
        pass
    building_done = add_label(building, label, kec)

    bID_kec_1 = building.bID_kec.nunique()
    bID_kec_2 = building_done.bID_kec.nunique()
    logging.info(f"bID_kec before : {bID_kec_1}, bID_kec after : {bID_kec_2}")

    building_done.to_csv(os.path.join(folder_out, f"{kec}_labeled.csv"), index=False)  # type: ignore


# for kec in kecamatan:
#     building = process(kec, label_gdf)
#     df_list.append(building)
#     logging.info(f"{kec} done")

# final_path = os.path.join(output_path, "Final_data.csv")
# final_df = pd.concat(df_list)
# print(final_df.columns)
# print(f"Total row : {len(final_df)}")
# final_df.to_csv(final_path)
