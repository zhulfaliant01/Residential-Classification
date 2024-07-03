import matplotlib.pyplot as plt
from utils import read_csv_to_wkt
import numpy as np
import os
import glob
import re

datal = "noleak"

folder = r"Data Modelling\testing_results"
files = (
    glob.glob(os.path.join(folder, "*_noleak_9.csv"))
    if datal == "noleak"
    else glob.glob(os.path.join(folder, "*Exp_91.csv"))
)
# Read CSV file

for file in files:
    name = re.search(r"([\w ]*)_noleak", file).group(1) if datal == "noleak" else re.search(r"([\w ]*)Exp_", file).group(1)  # type: ignore
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
        choices = [
            "Positif Benar",
            "Negatif Benar",
            "Positif Salah",
            "Negatif Salah",
        ]
        return np.select(conditions, choices)

    colors = {
        "Positif Benar": "#2ac1d1",
        "Negatif Benar": "#e280b8",
        "Positif Salah": "#d62a2b",
        "Negatif Salah": "#0e6cae",
    }

    # Apply the function to the dataframe
    df["conf_matrix"] = conf_matrix_vectorized(df["label"], df["pred_labels"])
    df["prob_loss"] = abs(df["pred_proba"] - df["label"])
    df["prob_loss"] = df["prob_loss"].apply(lambda x: x if x >= 0.518 else 0)
    # Plot dengan warna khusus
    df.to_csv(file, index=False)
    # # Create a color map for plotting
    # df["color"] = df["conf_matrix"].map(colors)  # type: ignore

    # # Plot with colors
    # fig, ax = plt.subplots(1, 1, figsize=(25, 25))
    # df.plot(ax=ax, color=df["color"])

    # # Custom legend
    # import matplotlib.patches as mpatches

    # legend_handles = [
    #     mpatches.Patch(color=color, label=label)
    #     for label, color in colors.items()
    # ]
    # ax.legend(handles=legend_handles, loc="best")
    # ax.set_axis_off()

    # # Save the plot
    # folder_save = (
    #     r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Dokumen\Hasil\No Local Data"
    #     if datal == "noleak"
    #     else r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Dokumen\Hasil\10% Local Data"
    # )
    print("saving...")
    # plt.savefig(os.path.join(folder_save, f"{name}.png"))
