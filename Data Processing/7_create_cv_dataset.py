import pandas as pd
from utils import read_csv_to_wkt
import json
import logging
import os
import glob
from sklearn.model_selection import train_test_split

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

with open("config.json", "r") as config_file:
    config = json.load(config_file)

cv_count = 5  # 5 or 10

if cv_count == 5:
    folder_in = config.get(f"create_training_cv_{cv_count}")["input"]
    folder_out = config.get(f"create_training_cv_{cv_count}")["output"]
    os.makedirs(folder_out, exist_ok=True)

    cross_val = {
        "cv1": ["Jagakarsa", "Setiabudi"],
        "cv2": ["Pancoran", "Pasar Minggu"],
        "cv3": ["Pesanggrahan", "Kebayoran Lama"],
        "cv4": ["Cilandak", "Mampang Prapatan", "Tebet"],
        "cv5": ["Kebayoran Baru", "Tebet", "Cilandak"],
    }

    files = glob.glob(os.path.join(folder_in, "*.csv"))
    for key, items in cross_val.items():
        logging.info(f"{key} start...")
        val_files = []
        train_files = []
        logging.info(f"{key}: start splitting...")
        # Split the training and validation in cv
        for file in files:
            if any(item in file for item in items):
                val_files.append(file)  # Validation
            else:
                train_files.append(file)  # Training

        logging.info(f"{key}: start read file...")
        cv_val_list = []
        for file in val_files:
            df = pd.read_csv(file)
            cv_val_list.append(df)

        cv_train_list = []
        for file in train_files:
            df = pd.read_csv(file)
            cv_train_list.append(df)

        logging.info(f"{key}: start concat...")
        val_df = pd.concat(cv_val_list, ignore_index=True)
        train_df = pd.concat(cv_train_list, ignore_index=True)

        logging.info(f"{key}: start exporting...")
        val_df.to_csv(os.path.join(folder_out, f"{key}_val.csv"), index=False)
        train_df.to_csv(os.path.join(folder_out, f"{key}_train.csv"), index=False)

        logging.info(f"{key}: Done!")


elif cv_count == 10:  # New 10-Fold CV configuration
    folder_in = config.get(f"create_training_cv_{cv_count}")["input"]
    folder_out = config.get(f"create_training_cv_{cv_count}")["output"]
    os.makedirs(folder_out, exist_ok=True)

    cross_val = {
        "cv1": ["Jagakarsa_1", "Setiabudi_1"],
        "cv2": ["Jagakarsa_2", "Setiabudi_2"],
        "cv3": ["Pasar Minggu_1", "Pancoran_1"],
        "cv4": ["Pasar Minggu_2", "Pancoran_2"],
        "cv5": ["Pesanggrahan_1", "Kebayoran Lama_1"],
        "cv6": ["Pesanggrahan_2", "Kebayoran Lama_2"],
        "cv7": ["Cilandak_1", "Mampang Prapatan_1", "Tebet_1"],
        "cv8": ["Cilandak_2", "Mampang Prapatan_2", "Tebet_2"],
        "cv9": ["Kebayoran Baru_1", "Tebet_3", "Cilandak_3"],
        "cv10": ["Kebayoran Baru_2", "Tebet_4", "Cilandak_4"],
    }

    # Files and data split
    files = glob.glob(os.path.join(folder_in, "2_*_building.csv"))

    def split_dataframe(df, num_splits):
        split_dfs = []
        for _ in range(num_splits - 1):
            part, df = train_test_split(
                df, test_size=1 / (num_splits - len(split_dfs)), random_state=42
            )
            split_dfs.append(part)
        split_dfs.append(df)
        return split_dfs

    # Mapping of original filenames to their respective dataframes split into parts
    split_data_mapping = {}

    for file in files:
        df = read_csv_to_wkt(file)
        if "Jagakarsa" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Setiabudi" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Pasar Minggu" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Pancoran" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Pesanggrahan" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Kebayoran Lama" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Cilandak" in file:
            split_data_mapping[file] = split_dataframe(df, 4)
        elif "Mampang Prapatan" in file:
            split_data_mapping[file] = split_dataframe(df, 2)
        elif "Tebet" in file:
            split_data_mapping[file] = split_dataframe(df, 4)
        elif "Kebayoran Baru" in file:
            split_data_mapping[file] = split_dataframe(df, 2)

    for key, items in cross_val.items():
        train_files = []
        val_files = []

        # Determine training and validation files based on split mapping
        for file, splits in split_data_mapping.items():
            for split, split_label in zip(
                splits,
                [
                    "{}_{}".format(file.replace("_building.csv", ""), i + 1)
                    for i in range(len(splits))
                ],
            ):
                if any(item in split_label for item in items):
                    val_files.append(split)
                else:
                    train_files.append(split)
        logging.info(f"{key}: start concat...")
        val_df = pd.concat(val_files, ignore_index=True)
        train_df = pd.concat(train_files, ignore_index=True)

        logging.info(f"{key}: start exporting...")
        val_df.to_csv(os.path.join(folder_out, f"{key}_val.csv"), index=False)
        train_df.to_csv(os.path.join(folder_out, f"{key}_train.csv"), index=False)

        logging.info(f"{key}: Done!")
