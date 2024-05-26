import geopandas as gpd
import glob
import os
import json
import momepy as mm
import re
import logging

del os.environ["PROJ_LIB"]

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set dataset type: "training" or "test"
dataset_type = "test"

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
folder_in = config.get(f"street_{dataset_type}_paths")["input"]
folder_out = config.get(f"street_{dataset_type}_paths")["output"]
os.makedirs(folder_out, exist_ok=True)
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

    if gdf.crs is None:
        gdf = gdf.set_crs(crs, allow_override=True)
        logging.info(f"{kec_name} : CRS = {gdf.crs}...")
        print(gdf.geometry[0])
    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)
        logging.info(f"{kec_name} : CRS = {gdf.crs}...")
        print(gdf.geometry[0])

    gdf = mm.remove_false_nodes(gdf)

    gdf["sID"] = range(1, len(gdf) + 1)

    output_file = os.path.join(folder_out, f"{kec_name}.csv")
    gdf.to_csv(output_file)


# Process all GeoJSON files in the input folder
files = glob.glob(os.path.join(folder_in, "*.geojson"))
for file in files:
    read_gdf(file)
