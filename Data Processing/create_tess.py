import geopandas as gpd
import momepy as mm
import glob
import os
import re
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

del os.environ["PROJ_LIB"]


def create_tess(file):
    name = re.search(r"([\w ]*)\_1.geojson", file).group(1)
    tessel_path = os.path.join(folder_out, f"{name}_tess.geojson")
    if os.path.exists(tessel_path):
        return
    else:
        logging.info("Starting %s!", file)
        gdf = gpd.read_file(file)
        crs = "EPSG:32748"
        if gdf.crs != crs:
            gdf = gdf.set_crs(crs, allow_override=True)

        limit = mm.buffered_limit(gdf, 100)

        logging.info("Starting tessellation : %s", name)
        try:
            tessellation = mm.Tessellation(
                gdf, "bID", limit=limit, verbose=True, use_dask=True
            )
            logging.info("Finish tessellation : %s", name)
        except Exception as e:
            logging.error("Failed in tessellation : %s - %s", name, e)
            raise

        tess = tessellation.tessellation

        tess.to_file(tessel_path, driver="GeoJSON")
        logging.info("Succeed: %s", name)


try:
    file_path = r"Data Collection\building_clean\validation"
    files = glob.glob(os.path.join(file_path, "*_1.geojson"))
    folder_out = r"Data Processing\tessellation\validation"
    os.makedirs(folder_out, exist_ok=True)

    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(create_tess, file) for file in files]

except Exception as e:
    logging.error("Failed in main: %s", e)
