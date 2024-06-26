import geopandas as gpd
import matplotlib.pyplot as plt
from utils import read_csv_to_wkt
import numpy as np
import os
import glob
import re

folder = r"Data Modelling\testing_results"
files = glob.glob(os.path.join(folder, "*_noleak_9.csv"))
# Read CSV file

for file in files:
    name = re.search(r"([\w ]*)_noleak", file).group(1)  # type: ignore
    df = read_csv_to_wkt(file)
    print(name)

    # Define the confusion matrix function
    def conf_matrix_vectorized(labels, preds):
        conditions = [
            (labels == 1) & (preds == 1),
            (labels == 0) & (preds == 0),
            (labels == 0) & (preds == 1),
            (labels == 1) & (preds == 0),
        ]
        choices = ["TP", "TN", "FP", "FN"]
        return np.select(conditions, choices)

    # Apply the function to the dataframe
    df["conf_matrix"] = conf_matrix_vectorized(df["label"], df["pred_labels"])
    df["prob_loss"] = abs(df["pred_proba"] - df["label"])
    df["prob_loss"] = df["prob_loss"].apply(lambda x: x if x >= 0.518 else 0)

    # Create the plot
    ax = df.plot("conf_matrix", legend=True, figsize=(10, 10))  # type: ignore
    ax.set_axis_off()

    # Save the plot
    folder_save = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Dokumen\Hasil\No Local Data"
    plt.savefig(os.path.join(folder_save, f"{name}.png"))
