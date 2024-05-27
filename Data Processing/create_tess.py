import geopandas as gpd
import momepy as mm
import glob
import os
import re
import logging
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

dataset_type = "test"

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
folder_in = config.get(f"tess_{dataset_type}_paths")["input"]
folder_out = config.get(f"tess_{dataset_type}_paths")["output"]
os.makedirs(folder_out, exist_ok=True)

crs = config.get("crs")


def create_tess(file):
    name = re.search(r"\\([\w ]*)\.shp", file).group(1)  # type: ignore
    logging.info(
        f"{name} Start...",
    )

    gdf = gpd.read_file(file)
    gdf["bID"] = range(1, len(gdf) + 1)  # Create a new bID to account of topology correction

    if gdf.crs != crs:
        gdf = gdf.set_crs(crs, allow_override=True)

    limit = mm.buffered_limit(gdf, 100)

    logging.info("Starting tessellation : %s", name)
    try:
        tessellation = mm.Tessellation(gdf, "bID", limit=limit, verbose=True, use_dask=True)
        logging.info("Finish tessellation : %s", name)
    except Exception as e:
        logging.error("Failed in tessellation : %s - %s", name, e)
        raise

    tess = tessellation.tessellation
    bID_list = tess.bID.to_list()
    gdf = gdf[~gdf.bID.isin(bID_list)]

    gdf.to_csv(file.replace(".shp", "_clean.csv"))  # update the bID
    tess.to_csv(os.path.join(folder_out, f"{name}_tessel.csv"))

    logging.info("Succeed: %s", name)


files = glob.glob(os.path.join(folder_in, "*.shp"))

for file in files:
    try:
        create_tess(file)
    except Exception as e:
        continue
