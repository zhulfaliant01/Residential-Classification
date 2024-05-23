import geopandas as gpd
import pandas as pd
import os
import logging

# Set logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

import os

del os.environ["PROJ_LIB"]  # Ada conflict antara PROJ dari venv dengan PROJ dari PostgreSQL


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


def process(kec: str, label_gdf):
    """
    - Adding bID_Kec label and update the calculated geojson
    - Safe it in a df, to add it all later (outside the function)
    - Find the label for each building
    ## Returns:
    - Master GDF that contains everything
    - DF that contains the calculated values with bID_Kec for ML
    """

    logging.info(f"{kec} start!")
    path = os.path.join(calculated_building_path, f"{kec}_calculated.geojson")
    out_path = os.path.join(output_path, f"{kec}_labeled.geojson")
    building = gpd.read_file(path)

    if building.crs != "EPSG:32748":
        building.set_crs("EPSG:32748", allow_override=True)

    xmin, ymin, xmax, ymax = building.total_bounds
    label = label_gdf.cx[xmin:xmax, ymin:ymax]

    if building.crs != "EPSG:32748":
        building.set_crs("EPSG:32748", allow_override=True)

    building = add_new_id(building, kec)
    building_done = add_label(building, label)

    bID_1 = building.bID.nunique()
    bID_2 = building_done.bID.nunique()
    logging.info(f"bID before : {bID_1}, bID after : {bID_2}")

    building_done.to_file(out_path, driver="GeoJSON")  # update the source JSON file

    return building_done.drop(
        columns=[
            "geometry",
            "bID",
            "sID",
            "mm_len",
            "nodeID",
            "node_start",
            "node_end",
        ],
        axis=1,
    )


df_list = []
kecamatan = [
    "Cilandak",
    "Jagakarsa",
    "Kebayoran Baru",
    "Kebayoran Lama",
    "Mampang Prapatan",
    "Pancoran",
    "Pasar Minggu",
    "Pesanggrahan",
    "Setiabudi",
    "Tebet",
]

kecamatan_validasi = ["Kelapa Gading"]

# Set path
logging.info("Setting path...")
calculated_building_path = r"Data Processing\Calculated_Building"
label_path = r"Data Collection\landuse_clean\jaksel_1.geojson"
output_path = r"Data Processing\labeled_building"
os.makedirs(output_path, exist_ok=True)

label_gdf = gpd.read_file(label_path)
label_gdf = label_gdf.explode(index_parts=True)

if label_gdf.crs != "EPSG:32748":
    label_gdf.set_crs("EPSG:32748", allow_override=True)
label_gdf = label_gdf[["label", "geometry"]]

for kec in kecamatan:
    building = process(kec, label_gdf)
    df_list.append(building)
    logging.info(f"{kec} done")

final_path = os.path.join(output_path, "Final_data.csv")
final_df = pd.concat(df_list)
print(final_df.columns)
print(f"Total row : {len(final_df)}")
final_df.to_csv(final_path)
