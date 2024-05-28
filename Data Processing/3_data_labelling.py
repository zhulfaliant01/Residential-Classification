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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set dataset type: "training" or "test"
dataset_type = "test"

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
folder_in = config.get(f"label_{dataset_type}_paths")["input"]
folder_out = config.get(f"label_{dataset_type}_paths")["output"]
landuse = config.get(f"landuse_{dataset_type}_paths")["input"]
os.makedirs(folder_out, exist_ok=True)

crs = config.get("crs")


def add_new_id(gdf, kec):
    logging.info("Add id start!")
    code_map = {
        "Cilandak": "CIL",
        "Jagakarsa": "JAG",
        "Kebayoran Baru": "KEB",
        "Kebayoran Lama": "KEL",
        "Mampang Prapatan": "MAP",
        "Pancoran": "PAN",
        "Pasar Minggu": "PAS",
        "Pesanggrahan": "PES",
        "Setiabudi": "SET",
        "Tebet": "TEB",
        "Kelapa Gading": "KGD",
        "Cilincing": "CLN",
        "Koja": "KOJ",
        "Pademangan": "PDM",
        "Penjaringan": "PJR",
        "Tanjung Priok": "TJP",
    }
    kec_code = code_map[kec]
    gdf["bID_kec"] = kec_code + gdf["bID"].astype(str)

    return gdf


def add_label(gdf, label):
    logging.info("Add label start!")

    intersections = gdf.overlay(label, how="intersection")

    df = intersections[["bID", "label", "geometry"]]
    df = df.dissolve(["bID", "label"])
    df.reset_index(inplace=True)
    df["inter_area"] = df.geometry.area

    result = df.groupby("bID").apply(
        lambda x: x.loc[x["inter_area"].idxmax()], include_groups=True
    )
    result = result.drop(columns="bID").reset_index()

    final_gdf = gdf.merge(
        result.drop(columns=["inter_area", "geometry"]), left_on="bID", right_on="bID"
    )

    final_gdf = gpd.GeoDataFrame(final_gdf)

    return final_gdf


files = glob.glob(os.path.join(folder_in, "*.shp"))
landuse_gdf = read_csv_to_wkt(glob.glob(os.path.join(landuse, "*.csv"))[0])
landuse_gdf = landuse_gdf.explode(index_parts=True)

if landuse_gdf.crs != crs:
    landuse_gdf = landuse_gdf.set_crs(crs, allow_override=True)  # type: ignore

for file in files:
    kec = re.search(r"\\([\w ]*).shp", file).group(1)  # type: ignore
    logging.info(f"{kec} start...")
    building = gpd.read_file(file)

    if building.crs != crs:
        building = building.set_crs(crs, allow_override=True)  # type: ignore

    xmin, ymin, xmax, ymax = building.total_bounds
    label = landuse_gdf.cx[xmin:xmax, ymin:ymax]

    building.geometry = building.geometry.buffer(0)
    building = building[building.geometry.is_valid]

    building = add_new_id(building, kec)
    building_done = add_label(building, label)

    bID_1 = building.bID.nunique()
    bID_2 = building_done.bID.nunique()
    logging.info(f"bID before : {bID_1}, bID after : {bID_2}")

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
