import pandas as pd
import os
from utils import read_csv_to_wkt
import json
import glob
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

dataset_type = "test"

with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

folder_in = config.get(f"merging_{dataset_type}_paths")["input"]
folder_out = config.get(f"merging_{dataset_type}_paths")["output"]

os.makedirs(folder_out, exist_ok=True)

files = glob.glob(os.path.join(folder_in, "*_building.csv"))

df_list = []
for file in files:
    name = re.search(r"\\([\w ])*_building.csv", file).group(1)  # type: ignore
    logging.info(f"{name} start...")
    df = read_csv_to_wkt(file)
    df = df.drop(columns=["highway_y", "node_start", "node_end", "mm_len", "nodeID", "sID"])

    if "highway_x" in df.columns:
        df = df.rename(columns={"highway_x": "highway"})

    df_list.append(df)

logging.info("Merging all...")
df_all = pd.concat(df_list, ignore_index=True)  # type: ignore
logging.info(f"Total Row : {df_all.shape[0]}")
df_all.to_csv(os.path.join(folder_out, "final_data.csv"), index=False)
