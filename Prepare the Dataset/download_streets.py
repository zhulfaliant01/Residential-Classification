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


def download_streets(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            graphs = osm.graph_from_place(place)
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\street"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_name = place.replace(", Jakarta Selatan, Indonesia", "")
        file_path = os.path.join(output_dir, f"streets_{file_name}.geojson")

        try:
            streets = osm.graph_to_gdfs(
                graphs,
                nodes=False,
                edges=True,
                node_geometry=False,
                fill_edge_geometry=True,
            )
            streets = streets[~streets.highway.isna()].reset_index(drop=True)
            streets = streets[["geometry", "highway"]]
            for column in streets.columns:
                for index, row in streets.iterrows():
                    if isinstance(row[column], list):
                        streets.loc[index, column] = ""
            streets.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


# Main function
# def main():
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [
#             executor.submit(download_streets, f"{kec}, {city}") for kec in kecamatan
#         ]


def download_streetsMampang(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Mampang Prapatan, Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            graphs = osm.graph_from_place(place)
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\street"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_path = os.path.join(output_dir, f"streets_desa_{kec}.geojson")

        try:
            streets = osm.graph_to_gdfs(
                graphs,
                nodes=False,
                edges=True,
                node_geometry=False,
                fill_edge_geometry=True,
            )
            streets = streets[~streets.highway.isna()].reset_index(drop=True)
            streets = streets[["geometry", "highway"]]
            for column in streets.columns:
                for index, row in streets.iterrows():
                    if isinstance(row[column], list):
                        streets.loc[index, column] = ""
            streets.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


# Khusus Mampang Prapatan
# def main():
#     AlamatLengkap = "Mampang Prapatan, Jakarta Selatan, Indonesia"
#     desa = ["Mampang Prapatan", "Tegal Parang", "Pela Mampang", "Bangka"]
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [
#             executor.submit(download_streetsMampang, f"{des}, {AlamatLengkap}")
#             for des in desa
#         ]
#         results = []
#         for done in as_completed(futures):
#             results.append(done)


def download_streetsJagakarsa(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Jagakarsa, Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            graphs = osm.graph_from_place(place)
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\street"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_path = os.path.join(output_dir, f"streets_desa_{kec}.geojson")

        try:
            streets = osm.graph_to_gdfs(
                graphs,
                nodes=False,
                edges=True,
                node_geometry=False,
                fill_edge_geometry=True,
            )
            streets = streets[~streets.highway.isna()].reset_index(drop=True)
            streets = streets[["geometry", "highway"]]
            for column in streets.columns:
                for index, row in streets.iterrows():
                    if isinstance(row[column], list):
                        streets.loc[index, column] = ""
            streets.to_file(file_path, driver="GeoJSON")
            print(f"Successfully exported to {file_path}")
        except Exception as e:
            print(f"Failed to export {kec}: {e}")


# def main():  # Jagakarsa
#     AlamatLengkap = "Jagakarsa, Jakarta Selatan, Indonesia"
#     desa = [
#         "Jagakarsa",
#         "Ciganjur",
#         "Lenteng Agung",
#         "Tanjung Barat",
#         "Srengseng Sawah",
#         "Cipedak",
#     ]
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         futures = [
#             executor.submit(download_streetsJagakarsa, f"{des}, {AlamatLengkap}")
#             for des in desa
#         ]


def download_streetsPesanggarahan(place):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kec = place.replace(", Pesanggrahan, Jakarta Selatan, Indonesia", "")
        print(f"{kec} Start!")
        try:
            graphs = osm.graph_from_place(place)
            print(f"Successfully downloaded {kec}")
        except Exception as e:
            print(f"Failed to download {kec}: {e}")

        # Ensure directory exists
        output_dir = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Prepare the Dataset\street"  # Adjusted to use forward slashes
        os.makedirs(
            output_dir, exist_ok=True
        )  # Create the directory if it doesn't exist

        # Sanitize file name
        file_path = os.path.join(output_dir, f"streets_desa_{kec}.geojson")

        try:
            streets = osm.graph_to_gdfs(
                graphs,
                nodes=False,
                edges=True,
                node_geometry=False,
                fill_edge_geometry=True,
            )
            streets = streets[~streets.highway.isna()].reset_index(drop=True)
            streets = streets[["geometry", "highway"]]
            for column in streets.columns:
                for index, row in streets.iterrows():
                    if isinstance(row[column], list):
                        streets.loc[index, column] = ""
            streets.to_file(file_path, driver="GeoJSON")
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
            executor.submit(download_streetsPesanggarahan, f"{des}, {AlamatLengkap}")
            for des in desa
        ]


if __name__ == "__main__":
    main()
