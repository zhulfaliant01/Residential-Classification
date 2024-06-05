import pandas as pd
from utils import read_csv_to_wkt
import json
import logging
import os
import glob

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

with open("config.json", "r") as config_file:
    config = json.load(config_file)

folder_in = config.get("create_training_cv")["input"]
folder_out = config.get("create_training_cv")["output"]
os.makedirs(folder_out, exist_ok=True)

cross_val = {
    "cv1": ["Jagakarsa", "Setiabudi"],
    "cv2": ["Pasar Minggu", "Pancoran"],
    "cv3": ["Pesanggrahan", "Kebayoran Lama"],
    "cv4": ["Cilandak", "Mampang Prapatan", "Tebet"],
    "cv5": ["Kebayoran Baru", "Tebet", "Cilandak"],
}

files = glob.glob(os.path.join(folder_in, "2_*_building.csv"))

for key, items in cross_val.items():
    new_files = files.copy()
    val_file = []

    # Split the training and validation in cv
    for file in new_files:
        if any(item in file for item in items):
            new_files.remove(file)  # Training
            val_file.append(file)  # Validation

    cv_val_list = []
    for file in val_file:
        df = read_csv_to_wkt(file)
        cv_val_list.append(df)

    cv_train_list = []
    for file in new_files:
        df = read_csv_to_wkt(file)
        cv_train_list.append(df)

    val_df = pd.concat(cv_val_list, ignore_index=True)
    train_df = pd.concat(cv_train_list, ignore_index=True)
    val_df.to_csv(os.path.join(folder_out, f"{key}_val.csv"), index=False)
    train_df.to_csv(os.path.join(folder_out, f"{key}_train.csv"), index=False)

    logging.info(f"{key}: Done!")
