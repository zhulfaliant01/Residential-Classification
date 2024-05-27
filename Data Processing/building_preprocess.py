import geopandas as gpd
import glob
import os
import json
import utils
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set dataset type: "training" or "test"
dataset_type = "training"

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
folder_in = config.get(f"building_{dataset_type}_paths")["input"]
folder_out = config.get(f"building_{dataset_type}_paths")["output"]
os.makedirs(folder_out, exist_ok=True)

# Set error path and coordinate reference system (CRS) from the configuration
error_path = config.get(f"building_error_{dataset_type}_paths")["path"]
os.makedirs(error_path, exist_ok=True)
crs = config.get("crs")


def read_gdf(file):
    """
    Reads a GeoJSON file into a GeoDataFrame, sets its CRS, assigns unique IDs, and saves it as a CSV.

    Args:
        file (str): Path to the GeoJSON file.

    Returns:
        gdf (GeoDataFrame): The processed GeoDataFrame.
        kec_name (str): Extracted name of the region from the file name.
    """
    kec_name = re.search(r"_([\w ]*)\.geojson", file).group(1)  # type: ignore
    logging.info(f"{kec_name} start...")
    gdf = gpd.read_file(file)

    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)
        logging.info(f"{kec_name} : CRS = {gdf.crs}...")
        print(gdf.geometry[0])

    gdf["bID"] = range(1, len(gdf) + 1)

    output_file = os.path.join(folder_out, f"{kec_name}.csv")
    gdf.to_csv(output_file)

    return gdf, kec_name


# Process all GeoJSON files in the input folder
files = glob.glob(os.path.join(folder_in, "*.geojson"))
for file in files:
    gdf, kec = read_gdf(file)

    # Check for overlap errors
    logging.info(f"{kec} : Overlap...")
    overlap_error = utils.check_overlap(gdf, "bID", 0.5)
    if overlap_error is not None:
        logging.info(f"{kec} : There is {len(overlap_error)} overlap...")
        overlap_path = os.path.join(error_path, f"{kec}_overlap.csv")
        overlap_error.to_csv(overlap_path)
    else:
        logging.info(f"{kec} : There is no overlap...")

    # Check for containment errors
    logging.info(f"{kec} : Containment...")
    contain_error = utils.check_containment(gdf, "bID", 0.5)
    if contain_error is not None:
        logging.info(f"{kec} : There is {len(contain_error)} containment...")
        contain_path = os.path.join(error_path, f"{kec}_contain.csv")
        contain_error.to_csv(contain_path)
    else:
        logging.info(f"{kec} : There is no containment...")
