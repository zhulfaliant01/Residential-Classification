import geopandas as gpd
import osmnx as osm
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import os

warnings.catch_warnings(action="ignore")

city = "Jakarta Selatan, Indonesia"
kecamatan = [
    "Cilandak",
    "Kebayoran Baru",
    "Kebayoran Lama",
    "Pancoran",
    "Pasar Minggu",
    "Setiabudi",
    "Tebet",
]


def download_building(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            buildings = osm.features_from_place(
                place, {"building": True, "type": "polygon"}
            )
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\building"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_name = place.replace(", Jakarta Selatan, Indonesia", "")
        file_path = os.path.join(output_dir, f"building_{file_name}.geojson")

        try:
            buildings = buildings[
                (buildings.geom_type == "Polygon")
                | (buildings.geom_type == "MultiPolygon")
            ].reset_index(drop=True)
            buildings = buildings[
                [
                    "name",
                    "building:levels",
                    "building:roof",
                    "building:walls",
                    "building",
                    "building:structure",
                    "amenity",
                    "geometry",
                ]
            ]
            for column in buildings.columns:
                for index, row in buildings.iterrows():
                    if isinstance(row[column], list):
                        buildings.loc[index, column] = ""

            buildings.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


# Main function
def main():
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(download_building, f"{kec}, {city}") for kec in kecamatan
        ]
        results = []
        for done in as_completed(futures):
            results.append(done)


def download_buildingMampang(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Mampang Prapatan, Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            buildings = osm.features_from_place(
                place, {"building": True, "type": "polygon"}
            )
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")
            return

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\building"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_name = place.replace(", Mampang Prapatan, Jakarta Selatan, Indonesia", "")
        file_path = os.path.join(output_dir, f"building_desa_{file_name}.geojson")

        try:
            buildings = buildings[
                (buildings.geom_type == "Polygon")
                | (buildings.geom_type == "MultiPolygon")
            ].reset_index(drop=True)
            buildings = buildings[
                [
                    "name",
                    "building:levels",
                    "building:roof",
                    "building:walls",
                    "building",
                    "building:structure",
                    "amenity",
                    "geometry",
                ]
            ]
            for column in buildings.columns:
                for index, row in buildings.iterrows():
                    if isinstance(row[column], list):
                        buildings.loc[index, column] = ""

            buildings.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


# Khusus Mampang Prapatan
# def main():
#     AlamatLengkap = "Mampang Prapatan, Jakarta Selatan, Indonesia"
#     desa = ["Mampang Prapatan", "Tegal Parang", "Pela Mampang", "Bangka"]
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [
#             executor.submit(download_buildingMampang, f"{des}, {AlamatLengkap}")
#             for des in desa
#         ]
#         results = []
#         for done in as_completed(futures):
#             results.append(done)


def download_buildingJagakarsa(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Jagakarsa, Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            buildings = osm.features_from_place(
                place, {"building": True, "type": "polygon"}
            )
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")
            return

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\building"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_path = os.path.join(output_dir, f"building_desa_{kec}.geojson")

        try:
            buildings = buildings[
                (buildings.geom_type == "Polygon")
                | (buildings.geom_type == "MultiPolygon")
            ].reset_index(drop=True)
            buildings = buildings[
                [
                    "name",
                    "building:levels",
                    "building:roof",
                    "building:walls",
                    "building",
                    "building:structure",
                    "amenity",
                    "geometry",
                ]
            ]
            for column in buildings.columns:
                for index, row in buildings.iterrows():
                    if isinstance(row[column], list):
                        buildings.loc[index, column] = ""

            buildings.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


def main():  # Jagakarsa
    AlamatLengkap = "Jagakarsa, Jakarta Selatan, Indonesia"
    desa = [
        "Jagakarsa",
        "Ciganjur",
        "Lenteng Agung",
        "Tanjung Barat",
        "Srengseng Sawah",
        "Cipedak",
    ]
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(download_buildingJagakarsa, f"{des}, {AlamatLengkap}")
            for des in desa
        ]


def download_buildingPesanggrahan(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Pesanggrahan, Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            buildings = osm.features_from_place(
                place, {"building": True, "type": "polygon"}
            )
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")
            return

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\building"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_path = os.path.join(output_dir, f"building_desa_{kec}.geojson")

        try:
            buildings = buildings[
                (buildings.geom_type == "Polygon")
                | (buildings.geom_type == "MultiPolygon")
            ].reset_index(drop=True)
            buildings = buildings[
                [
                    "name",
                    "building:levels",
                    "building:roof",
                    "building:walls",
                    "building",
                    "building:structure",
                    "amenity",
                    "geometry",
                ]
            ]
            for column in buildings.columns:
                for index, row in buildings.iterrows():
                    if isinstance(row[column], list):
                        buildings.loc[index, column] = ""

            buildings.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


def main():  # Pesanggrahan
    AlamatLengkap = "Pesanggrahan, Jakarta Selatan, Indonesia"
    desa = [
        "Ulujami",
        "Pesanggrahan",
        "Petukangan Utara",
        "Petukangan Selatan",
        "Bintaro",
    ]
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(download_buildingPesanggrahan, f"{des}, {AlamatLengkap}")
            for des in desa
        ]


if __name__ == "__main__":
    main()
