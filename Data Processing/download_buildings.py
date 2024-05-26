import geopandas as gpd
import osmnx as osm
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import os

warnings.catch_warnings(action="ignore")

city = "Jakarta Utara, Indonesia"
kecamatan = ["Pademangan", "Penjaringan"]


def download_building(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Jakarta Utara, Indonesia", "")
        print(f"{kec} Start!")
        osm.config(timeout=300)
        try:
            buildings = osm.features_from_place(place, {"building": True, "type": "polygon"})
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")

        # Ensure directory exists
        output_dir = r"Data Collection\building\validation"  # Adjusted to use forward slashes
        os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist

        # Sanitize file name
        file_name = kec
        file_path = os.path.join(output_dir, f"building_{file_name}.csv")

        try:
            buildings = buildings[
                (buildings.geom_type == "Polygon") | (buildings.geom_type == "MultiPolygon")
            ].reset_index(drop=True)
            buildings = buildings[["building", "geometry"]]
            for column in buildings.columns:
                for index, row in buildings.iterrows():
                    if isinstance(row[column], list):
                        buildings.loc[index, column] = ""
            print(buildings.shape[0])
            buildings.to_csv(file_path)
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


# Main function
def main():
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(download_building, f"{kec}, {city}") for kec in kecamatan]
        results = []
        for done in as_completed(futures):
            results.append(done)
