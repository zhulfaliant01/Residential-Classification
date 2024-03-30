# This python script is used to change the projection system of the dataset.
# Also, do some pre-processing

import geopandas as gpd
import momepy as mm
import os
import glob
import re
import logging
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load configuration
try:
    with open("Data Processing\config.json", "r") as config_file:
        config = json.load(config_file)
except Exception as e:
    logging.error("Failed to load config: %s", e)
    raise SystemExit(e)

crs = config.get("target_crs", "EPSG:32748")


def clean_data(path, folder_out, data_type):
    try:
        name = re.search(r"_([\w ]*)\.geojson", path).group(1)
        final_path = os.path.join(folder_out, f"{name}_1.geojson")
        gdf = gpd.read_file(path)

        if gdf.crs != crs:
            gdf = gdf.to_crs(crs)

        if data_type == "landuse":
            gdf = gdf.dissolve("label")
        elif data_type == "building":
            gdf["bID"] = range(1, len(gdf) + 1)
        elif data_type == "street":
            gdf = mm.remove_false_nodes(gdf)
            gdf["sID"] = range(1, len(gdf) + 1)

        gdf.to_file(final_path, driver="GeoJSON")
    except Exception as e:
        logging.error("Error processing %s in %s: %s", data_type, path, e)


def process_files(folder_in, folder_out, data_type):
    try:
        os.makedirs(folder_out, exist_ok=True)
        files = glob.glob(os.path.join(folder_in, "*.geojson"))
        for file in files:
            clean_data(file, folder_out, data_type)
    except Exception as e:
        logging.error("Error processing files in %s: %s", folder_in, e)


def main():
    logging.info("Processing Start...")

    landuse_paths = config.get("landuse_paths", {})
    building_paths = config.get("building_paths", {})
    street_paths = config.get("street_paths", {})

    clean_data(landuse_paths["input"], landuse_paths["output"], "landuse")
    process_files(building_paths["input"], building_paths["output"], "building")
    process_files(street_paths["input"], street_paths["output"], "street")

    logging.info("Processing Done...")


if __name__ == "__main__":
    main()
