# Untuk merge data bangunan
import geopandas as gpd
import os
import pandas as pd


def merge_streets(name, list):

    gdf_list = []
    for file in list:
        df = gpd.read_file(file)
        gdf_list.append(df)

    gdf = gpd.GeoDataFrame(pd.concat(gdf_list))
    path = r"C:\Users\Lenovo\OneDrive - UGM 365\Documents\Second Brain\1 Projects\Skripsi\Code\Residential-Classification\Data Collection\street_clean"
    path = os.path.join(path, f"{name}_clean.geojson")
    gdf.to_file(path, driver="GeoJSON")

    for file in list:
        os.remove(file)


def main():

    list_mampang = [
        r"Data Collection\street_clean\desa_Mampang Prapatan_clean.geojson",
        r"Data Collection\street_clean\desa_Tegal Parang_clean.geojson",
        r"Data Collection\street_clean\desa_Pela Mampang_clean.geojson",
        r"Data Collection\street_clean\desa_Bangka_clean.geojson",
    ]
    merge_streets("Mampang Prapatan", list_mampang)

    list_jagakarsa = [
        r"Data Collection\street_clean\desa_Jagakarsa_clean.geojson",
        r"Data Collection\street_clean\desa_Ciganjur_clean.geojson",
        r"Data Collection\street_clean\desa_Lenteng Agung_clean.geojson",
        r"Data Collection\street_clean\desa_Tanjung Barat_clean.geojson",
        r"Data Collection\street_clean\desa_Srengseng Sawah_clean.geojson",
        r"Data Collection\street_clean\desa_Cipedak_clean.geojson",
    ]
    merge_streets("Jagakarsa", list_jagakarsa)

    list_pesanggrahan = [
        r"Data Collection\street_clean\desa_Bintaro_clean.geojson",
        r"Data Collection\street_clean\desa_Pesanggrahan_clean.geojson",
        r"Data Collection\street_clean\desa_Petukangan Selatan_clean.geojson",
        r"Data Collection\street_clean\desa_Petukangan Utara_clean.geojson",
        r"Data Collection\street_clean\desa_Ulujami_clean.geojson",
    ]
    merge_streets("Pesanggrahan", list_pesanggrahan)


if __name__ == "__main__":
    main()
