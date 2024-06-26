"""
- One Hot Encoding
- Remove missing values
- Imputing missing values if possible

Input : Calculated labeled data
Output :
    - Updated labeled data with predictors, labels, and id -> going to be used for cross-validation later
    - A merged dataset (training and testing)
"""

import pandas as pd
import os
import json
import logging
import glob

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

dataset_type = "test"

with open("config.json", "r") as config_file:
    config = json.load(config_file)

folder_in = config.get(f"preprocess_{dataset_type}_paths")["input"]
folder_out = config.get(f"preprocess_{dataset_type}_paths")["output"]
folder_out_merged = config.get(f"preprocess_{dataset_type}_paths")["output_merged"]
os.makedirs(folder_out, exist_ok=True)
os.makedirs(folder_out_merged, exist_ok=True)

# Open the data
files = glob.glob(os.path.join(folder_in, "*_labeled.csv"))

df_list = []
for file in files:
    name = os.path.basename(file).replace("_labeled.csv", "")
    df = pd.read_csv(file)
    total_row_a = df.shape[0]

    # Remove building with less than 9m2 areas and 12m perimeter --------------------------------
    df = df[(df.b_area >= 9) & (df.b_perimeter >= 12)]
    total_row_b = df.shape[0]

    logging.info(
        f"{name}: Before buildings removal: {total_row_a} rows, After buildings removal: {total_row_b} rows"
    )

    # Remove useless columns --------------------------------
    total_column_a = df.shape[1]
    df = df.drop(
        columns=[
            "node_start",
            "node_end",
            "index_right",
            "sID",
            "index",
        ],  # Tinggal bID_kec, geometry, and labels
        axis=1,
    )
    df.drop(columns="nodeID") if name == "Cilandak" else None

    total_column_b = df.shape[1]
    logging.info(
        f"{name}: Before columns removal: {total_column_a} columns, After columns removal: {total_column_b} columns"
    )

    # Reclassify and one hot encodings
    reclass_map_building = {
        "yes": "yes",
        "sports_centre": "non_residential",
        "apartments": "residential",
        "hospital": "non_residential",
        "mosque": "non_residential",
        "school": "non_residential",
        "government_office": "non_residential",
        "clinic": "non_residential",
        "industrial": "non_residential",
        "office": "non_residential",
        "village_office": "non_residential",
        "church": "non_residential",
        "power_substation": "non_residential",
        "university": "non_residential",
        "college": "non_residential",
        "kindergarten": "non_residential",
        "house": "residential",
        "hotel": "non_residential",
        "marketplace": "non_residential",
        "community_group_office": "non_residential",
        "subdistrict_office": "non_residential",
        "residential": "residential",
        "retail": "non_residential",
        "commercial": "non_residential",
        "garage": "non_residential",
        "bank": "non_residential",
        "detached": "residential",
        "fire_station": "non_residential",
        "supermarket": "non_residential",
        "animal_boarding": "non_residential",
        "police": "non_residential",
        "roof": "non_residential",
        "pumping_station": "non_residential",
        "post_office": "non_residential",
        "convenience": "non_residential",
        "service": "non_residential",
        "train_station": "non_residential",
        "construction": "non_residential",
        "library": "non_residential",
        "parking": "non_residential",
        "pharmacy": "non_residential",
        "terrace": "residential",
        "greengrocer": "non_residential",
        "public": "non_residential",
        "mall": "non_residential",
        "government": "non_residential",
        "transportation": "non_residential",
        "civic": "non_residential",
        "power_plant": "non_residential",
        "temple": "non_residential",
        "grandstand": "non_residential",
        "warehouse": "non_residential",
        "restaurant": "non_residential",
        "embassy": "non_residential",
        "institution": "non_residential",
        "ruins": "non_residential",
        "dormitory": "residential",
        "social_facility": "non_residential",
        "hut": "non_residential",
        "shed": "non_residential",
    }
    reclass_map_street = {
        "residential": "Residential",
        "living_street": "Residential",
        "tertiary": "Major Roads",
        "secondary": "Major Roads",
        "primary": "Major Roads",
        "unclassified": "Major Roads",
        "trunk": "Major Roads",
        "trunk_link": "Major Roads",
        "primary_link": "Major Roads",
        "secondary_link": "Major Roads",
        "tertiary_link": "Major Roads",
        "motorway": "Major Roads",
        "motorway_link": "Major Roads",
        "service": "Service Roads",
        "services": "Service Roads",
        "footway": "Pedestrian and Bicycle Paths",
        "path": "Pedestrian and Bicycle Paths",
        "cycleway": "Pedestrian and Bicycle Paths",
        "pedestrian": "Pedestrian and Bicycle Paths",
        "steps": "Pedestrian and Bicycle Paths",
        "track": "Other",
        "busway": "Other",
    }

    df["building"] = df["building"].map(reclass_map_building).fillna("yes")
    df["highway"] = df["highway"].map(reclass_map_street).fillna("Other")

    df = pd.get_dummies(df, columns=["building", "highway"])
    total_column_c = df.shape[1]

    logging.info(
        f"{name}: Before OneHotEncoding: {total_column_b} columns, After OneHotEncoding: {total_column_c} columns"
    )

    # Export to csv file
    df.to_csv(os.path.join(folder_out, f"{name}.csv"), index=False)
    df_list.append(df)  # to be merged later

    logging.info(f"{name}: Exported to {os.path.join(folder_out, f'{name}.csv')}")

if dataset_type == "training":
    logging.info(f"Merging {dataset_type} dataset...")
    df_merged = pd.concat(df_list, ignore_index=True)
    logging.info(f"Total rows: {df_merged.shape[0]}")
    df_merged.to_csv(os.path.join(folder_out_merged, f"{dataset_type}_merged.csv"))
