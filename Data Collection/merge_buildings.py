# Untuk merge data bangunan
import geopandas as gpd
import os
import pandas as pd


def merge_building(name, list):

    gdf_list = []
    for file in list:
        df = gpd.read_file(file)
        gdf_list.append(df)

    gdf = gpd.GeoDataFrame(pd.concat(gdf_list))
    path = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Data Collection\building"
    path = os.path.join(path, f"{name}.geojson")
    gdf.to_file(path, driver="GeoJSON")


def main():

    list_mampang = [
        r"Data Collection\building\building_desa_Mampang Prapatan.geojson",
        r"Data Collection\building\building_desa_Tegal Parang.geojson",
        r"Data Collection\building\building_desa_Pela Mampang.geojson",
        r"Data Collection\building\building_desa_Bangka.geojson",
    ]
    merge_building("Mampang Prapatan", list_mampang)

    list_jagakarsa = [
        r"Data Collection\building\building_desa_Jagakarsa.geojson",
        r"Data Collection\building\building_desa_Ciganjur.geojson",
        r"Data Collection\building\building_desa_Lenteng Agung.geojson",
        r"Data Collection\building\building_desa_Tanjung Barat.geojson",
        r"Data Collection\building\building_desa_Srengseng Sawah.geojson",
        r"Data Collection\building\building_desa_Cipedak.geojson",
    ]
    merge_building("Jagakarsa", list_jagakarsa)

    list_pesanggrahan = [
        r"Data Collection\building\building_desa_Bintaro.geojson",
        r"Data Collection\building\building_desa_Pesanggrahan.geojson",
        r"Data Collection\building\building_desa_Petukangan Selatan.geojson",
        r"Data Collection\building\building_desa_Petukangan Utara.geojson",
        r"Data Collection\building\building_desa_Ulujami.geojson",
    ]
    merge_building("Pesanggrahan", list_pesanggrahan)


if __name__ == "__main__":
    main()
