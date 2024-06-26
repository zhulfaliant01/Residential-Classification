#!/usr/bin/env python

# dimension.py
import pandas as pd
import geopandas as gpd
from shapely import wkt
import fiona
import json
import gc
import numpy as np

__all__ = [
    "read_csv_to_wkt",
    "del_gc",
    "mm_std_character",
    "mm_total_area",
    "street_centrality_value",
    "check_and_set_crs",
    "find_street_fr_building",
    "mm_street_character",
    "mm_count_intersections",
    "check_correct_multipart",
]


def read_csv_to_wkt(file, geom_column="geometry", index_col=None):
    """
    Reads a CSV file and converts its geometry column to Well-Known Text (WKT) format.

    Parameters:
    - file (str): The path to the CSV file.
    - geom_column (str, optional): The name of the geometry column in the CSV file. Defaults to "geometry".
    - index_col (str or list, optional): The name or list of names of the index column(s) in the CSV file. If not provided, it defaults to no index column.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame object with the geometry column converted to WKT format.

    Raises:
    - ValueError: If the CSV file does not contain a geometry column or if the geometry column contains null values.

    Example:
    ```
    df = read_csv_to_wkt('path/to/your/csv_file.csv')
    ```
    """
    print("Reading CSV file...")

    if index_col is None:
        data = pd.read_csv(file)
    else:
        data = pd.read_csv(file, index_col=index_col)

    print(f"Data loaded: {data.shape[0]} rows and {data.shape[1]} columns")

    print("Applying WKT loads...")
    null_data = data.geometry.isna().sum()
    if null_data:
        print(f"There is {null_data} null geometry")
    data = data.dropna(subset=[geom_column])

    data[geom_column] = data[geom_column].astype(str)
    data[geom_column] = data[geom_column].apply(wkt.loads)
    print(f"WKT conversion done: {data[geom_column].notnull().sum()} geometries loaded")

    print("Converting to GeoDataFrame...")
    data = gpd.GeoDataFrame(data, geometry=geom_column)  # type: ignore
    print("Conversion to GeoDataFrame done.")

    return data


def del_gc(var):
    del var
    gc.collect()


def mm_std_character(gdf: gpd.GeoDataFrame, sw, value: str, unique_id: str):
    data = gdf.copy()
    data = data.set_index(unique_id)[value]
    std = []
    for index in data.index:
        if index in sw.neighbors:
            neighbors = [index]
            neighbors += sw.neighbors[index]

            values_list = data.loc[neighbors]
            std.append(np.std(values_list))
        else:
            std.append(np.nan)

    std = pd.Series(std, index=gdf.index)
    return std


def mm_total_area(gdf, sw, unique_id):
    """
    Calculates the total area of neighboring buildings for each building in a GeoDataFrame.

    Parameters:
    - gdf (gpd.GeoDataFrame): A GeoDataFrame containing building geometries and attributes.
    - sw (SpatialWeights object): A SpatialWeights object representing the spatial relationships between buildings.
    - unique_id (str): The name of the column in the GeoDataFrame that uniquely identifies each building.

    Returns:
    - total_area (pd.Series): A Series containing the total area of neighboring buildings for each building.

    Example:
    ```
    total_area = mm_total_area(gdf, sw, "building_id")
    ```
    """
    data = gdf.copy()
    data = data.set_index(unique_id)["b_area"]
    total_area = []
    for index in data.index:
        if index in sw.neighbors:
            neighbors = [index]
            neighbors += sw.neighbors[index]
            values_list = data.loc[neighbors]
            total_area.append(np.sum(values_list))
        else:
            total_area.append(np.nan)
    total_area = pd.Series(total_area, index=gdf.index)
    return total_area


def list_gdb_layers(gdb_file):
    layers = fiona.listlayers(gdb_file)
    return layers


def street_centrality_value(street, nodes, value):
    edges = street[["sID", "node_start", "node_end"]].copy()
    node = nodes[["nodeID", value]].copy()

    node = node.set_index("nodeID")[value]
    values = []
    for _, edge in edges.iterrows():
        edge_node = [edge.node_start, edge.node_end]
        node_list = node.loc[edge_node]

        avg_value = np.mean(node_list)
        values.append(avg_value)

    values = pd.Series(values, index=street.index)
    return values


def find_street_fr_building(building, street, dist, building_id, street_id, debug=False):
    building = building[[building_id, "geometry"]].copy()
    street_data = street[[street_id, "geometry"]].copy()

    # Convert building geometries to centroids
    building["centroid"] = building.centroid
    building = building.set_index(building_id)
    street_data = street_data.set_index(street_id)

    # Buffer around building centroids
    buffer = building["centroid"].buffer(dist)
    sindex = street_data.sindex

    print("Start indexing...")

    final_result = []
    for index in building.index:
        hits = sindex.query(buffer[index], predicate="intersects")
        print(f"Index {index}, hits: {hits}") if debug == True else None

        if len(hits) == 0:
            match = []
            final_result.append(match)
            continue

        if index == "PJR1353" and dist == 50:
            match = []
            final_result.append(match)
            continue

        if hits.ndim == 2:
            hits = hits.flatten()

        hits = np.unique(hits)
        try:
            match = street_data.iloc[hits].index.to_list()
        except:
            match = []
            final_result.append(match)
            continue
        final_result.append(match)

    print("Create series...")
    final_result = pd.Series(final_result, index=building.index)
    return final_result


def check_correct_multipart(gdf, ids, geom_type):
    n_gdf = gdf.copy()
    if geom_type in n_gdf.geom_type.tolist():
        n_gdf = n_gdf.explode(index_parts=False)
        n_gdf = n_gdf.drop_duplicates(ids)
    return n_gdf


def mm_street_character(gdf, street, value, sw, bID, sID, mode: list, dist, debug=False):
    data = gdf.copy()
    street = street.copy()
    data = data.set_index(bID)
    street = street.set_index(sID)[value]

    mean = []
    max = []
    total = []
    std = []

    print("Start indexing...")
    for index in data.index:
        if index in sw:
            street_neigh = sw[index]
            try:
                if isinstance(street_neigh, list):
                    values_list = street.loc[street_neigh].values
                else:
                    values_list = [street.loc[street_neigh]]
            except:
                if "mean" in mode:
                    mean.append(np.nan)
                if "max" in mode:
                    max.append(np.nan)
                if "total" in mode:
                    total.append(np.nan)
                if "std" in mode:
                    std.append(np.nan)
                continue

            print(f"Index: {index}; Values: {values_list}") if debug == True else None

            if len(values_list) == 0 or (index == "CIL360" and dist == 50):
                if "mean" in mode:
                    mean.append(np.nan)
                if "max" in mode:
                    max.append(np.nan)
                if "total" in mode:
                    total.append(np.nan)
                if "std" in mode:
                    std.append(np.nan)
                continue

            if "mean" in mode:
                mean.append(np.mean(values_list))
            if "max" in mode:
                max.append(np.max(values_list))
            if "total" in mode:
                total.append(np.sum(values_list))
            if "std" in mode:
                std.append(np.std(values_list))
        else:
            if "mean" in mode:
                mean.append(np.nan)
            if "max" in mode:
                max.append(np.nan)
            if "total" in mode:
                total.append(np.nan)
            if "std" in mode:
                std.append(np.nan)

    if "mean" in mode:
        mean = pd.Series(mean, index=gdf.index)
    if "max" in mode:
        max = pd.Series(max, index=gdf.index)
    if "total" in mode:
        total = pd.Series(total, index=gdf.index)
    if "std" in mode:
        std = pd.Series(std, index=gdf.index)

    if "mean" in mode and "max" in mode:
        return mean, max

    elif "mean" in mode and "total" in mode and "std" in mode:
        return mean, total, std

    elif "mean" in mode and "std" in mode:
        return mean, std


def mm_count_intersections(gdf, intersection, dist, unique_id):
    building = gdf.copy()
    building = building.set_index(unique_id)
    building.geometry = building.centroid

    buffer = building.geometry.buffer(dist)

    sindex = intersection.sindex

    counts = []
    for index in building.index:
        hits = sindex.query(buffer.loc[index], predicate="intersects")
        count = len(hits)
        counts.append(count)

    counts = pd.Series(counts, index=gdf.index)
    return counts


def check_and_set_crs(gdf: gpd.GeoDataFrame, crs):
    if gdf.crs != crs:
        gdf.set_crs(crs, allow_override=True)
    return gdf


def gdb_to_csv(gdb_file, layer_name, output_filename):
    pass
