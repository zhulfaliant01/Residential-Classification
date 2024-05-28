import os
import geopandas as gpd
import pandas as pd
import momepy as mm
import glob
import logging
import libpysal
import warnings
import json
import re
import gc
from utils import read_csv_to_wkt, check_and_set_crs

# del os.environ["PROJ_LIB"]  # Ada conflict antara PROJ dari venv dengan PROJ dari PostgreSQL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

dataset_type = "test"  # Sebelum hitung training, data jalannya perlu dipecah dulu

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
building_in = config.get(f"morphometric_{dataset_type}_paths")["input_building"]
street_in = config.get(f"morphometric_{dataset_type}_paths")["input_street"]
tessel_in = config.get(f"morphometric_{dataset_type}_paths")["input_tessel"]
folder_out = config.get(f"morphometric_{dataset_type}_paths")["output"]
folder_out_final = config.get(f"morphometric_{dataset_type}_paths")["output_final"]

os.makedirs(folder_out, exist_ok=True)
os.makedirs(folder_out_final, exist_ok=True)

crs = config.get("crs")


def dimension(building, tessel, street, sw3):
    logging.info(f"{name}: Dimension...")
    building_gdf = building
    tessel_gdf = tessel
    street_gdf = street

    building_gdf["b_area"] = building_gdf.area
    building_gdf["b_perimeter"] = mm.Perimeter(building_gdf).series
    building_gdf["b_lal"] = mm.LongestAxisLength(building_gdf).series

    tessel_gdf["t_area"] = tessel_gdf.area
    tessel_gdf["t_perimeter"] = mm.Perimeter(tessel_gdf).series
    tessel_gdf["t_cov_area"] = mm.CoveredArea(tessel_gdf, sw3, "bID_kec", False).series

    street_gdf["s_sum_length"] = mm.SegmentsLength(street_gdf, verbose=False).sum
    street_gdf["s_length"] = street_gdf.length

    """
    We can add another averaged character to learn how the average feature around
    the building with mm.AverageCharacter
    """
    return building_gdf, tessel_gdf, street_gdf


def shape(building, tessel, street):
    logging.info(f"{name}: Shape...")
    building_gdf = building
    tessel_gdf = tessel
    street_gdf = street

    building_gdf["b_convexity"] = mm.Convexity(building_gdf, "b_area").series
    ccd = mm.CentroidCorners(building_gdf, verbose=False)
    building_gdf["b_ccd_means"] = ccd.mean
    building_gdf["b_ccd_std"] = ccd.std
    building_gdf["b_circular_comp"] = mm.CircularCompactness(
        building_gdf, areas="b_area"
    ).series
    building_gdf["b_square_comp"] = mm.SquareCompactness(building_gdf, areas="b_area").series
    building_gdf["b_eri"] = mm.EquivalentRectangularIndex(
        building_gdf, "b_area", "b_perimeter"
    ).series
    building_gdf["b_squareness"] = mm.Squareness(building_gdf, verbose=False).series
    building_gdf["b_corners"] = mm.Corners(building_gdf, verbose=False).series
    building_gdf["b_elong"] = mm.Elongation(building_gdf).series

    tessel_gdf["t_convexity"] = mm.Convexity(tessel_gdf).series
    tessel_gdf["t_circular_comp"] = mm.CircularCompactness(tessel_gdf).series
    tessel_gdf["t_square_comp"] = mm.SquareCompactness(tessel_gdf).series
    tessel_gdf["t_eri"] = mm.EquivalentRectangularIndex(tessel_gdf).series
    tessel_gdf["t_squareness"] = mm.Squareness(tessel_gdf, False).series

    street_gdf["s_linearity"] = mm.Linearity(street_gdf).series
    profile = mm.StreetProfile(street_gdf, building_gdf)
    street_gdf["s_width"] = profile.w
    street_gdf["s_width_def"] = profile.wd
    street_gdf["s_openness"] = profile.o

    return building_gdf, tessel_gdf, street_gdf


def spatial_distribution(building, tessel, street, sw1, sw3):
    logging.info(f"{name}: Spatial Distribution...")
    building_gdf = building
    tessel_gdf = tessel
    street_gdf = street

    ## Distance Band - Cocok untuk hubungan spasial yang bersifat continues
    logging.info(f"{name}: Create distance spatial weights...")

    building_gdf["b_adjacency"] = mm.BuildingAdjacency(
        building_gdf, sw3, "bID_kec", verbose=False
    ).series  # Using building spatial weight and tessel sw higher

    del sw3
    gc.collect()

    dist = libpysal.weights.DistanceBand.from_dataframe(building_gdf, 50, ids="bID_kec")
    building_gdf["b_neighbor_50"] = mm.Neighbors(
        building_gdf, dist, "bID_kec", verbose=False
    ).series

    dist = libpysal.weights.DistanceBand.from_dataframe(building_gdf, 150, ids="bID_kec")
    building_gdf["b_neighbor_150"] = mm.Neighbors(
        building_gdf, dist, "bID_kec", verbose=False
    ).series

    dist = libpysal.weights.DistanceBand.from_dataframe(building_gdf, 300, ids="bID_kec")
    building_gdf["b_neighbor_300"] = mm.Neighbors(
        building_gdf, dist, "bID_kec", verbose=False
    ).series

    del dist
    gc.collect()

    # Either use mean interbuilding distance or neighbour distance
    building_gdf["b_neigh_dis"] = mm.NeighborDistance(
        building_gdf, sw1, "bID_kec", False
    ).series
    building_gdf["b_orientation"] = mm.Orientation(building_gdf, False).series
    building_gdf["b_alignment"] = mm.Alignment(
        building_gdf, sw1, "bID_kec", "b_orientation", False
    ).series

    del sw1
    gc.collect()

    tessel_gdf["t_orientation"] = mm.Orientation(tessel_gdf, False).series
    tessel_gdf["t_alignment"] = mm.CellAlignment(
        building_gdf, tessel_gdf, "b_orientation", "t_orientation", "bID_kec", "bID_kec"
    ).series

    street_gdf["s_alignment"] = mm.StreetAlignment(
        building_gdf, street_gdf, "b_orientation", network_id="sID"
    ).series

    return (
        building_gdf.drop(columns=["b_orientation"]),
        tessel_gdf.drop(columns=["t_orientation"]),
        street_gdf,
    )


def intensity(building, tessel, street):
    logging.info(f"{name}: Intensity...")
    building_gdf = building
    tessel_gdf = tessel
    street_gdf = street

    building_gdf["b_area_ratio"] = mm.AreaRatio(
        tessel_gdf, building_gdf, "t_area", "b_area", "bID_kec"
    ).series

    street_gdf["s_reached"] = mm.Reached(
        street_gdf, building_gdf, "sID", "sID", verbose=False
    ).series

    return building_gdf, tessel_gdf, street_gdf


def diversity(building):
    return


def graph(building, street):
    logging.info(f"{name}: Graph...")
    building_gdf = building
    street_gdf = street

    graph = mm.gdf_to_nx(street_gdf)
    graph = mm.node_degree(graph, "n_degree")
    logging.info(f"{name}: Betweenness...")
    # Seberapa terhubung sebuah titik di jaringan jalan perkotaan
    graph = mm.betweenness_centrality(
        graph, "n_betwenness", radius=500, distance="mm_len", verbose=False
    )
    logging.info(f"{name}: Closeness...")
    # Seberapa terpusat sebuah titik di jaringan jalan perkotaan
    graph = mm.closeness_centrality(
        graph, name="n_closeness", radius=500, distance="mm_len", verbose=False
    )
    # logging.info("Straightness...")
    # graph = mm.straightness_centrality(
    #     graph, "mm_len", False, "n_straightness", 500, verbose=False
    # )
    logging.info(f"{name}: Meshedness...")
    graph = mm.meshedness(graph, name="n_meshed", radius=500, distance="mm_len", verbose=False)

    nodes_gdf, street_gdf = mm.nx_to_gdf(graph)  # type: ignore

    building_gdf["nodeID"] = mm.get_node_id(
        building_gdf, nodes_gdf, street_gdf, "nodeID", "sID", verbose=False
    )

    return building_gdf, nodes_gdf, street_gdf


building_files = glob.glob(os.path.join(building_in, "*_final.csv"))[-1:]
street_files = glob.glob(os.path.join(street_in, "*_drive.csv"))[-1:]
tessel_files = glob.glob(os.path.join(tessel_in, "*_tessel.csv"))[-1:]

df_list = []
for building, street, tessel in zip(building_files, street_files, tessel_files):
    name = re.search(r"\\([\w ]*)_final.csv", building).group(1)  # type: ignore
    try:
        logging.info(f"{name} start...")

        building_gdf = check_and_set_crs(read_csv_to_wkt(building), crs)
        street_gdf = check_and_set_crs(read_csv_to_wkt(street), crs)
        tessel_gdf = check_and_set_crs(read_csv_to_wkt(tessel), crs)

        building_gdf = building_gdf[["bID_kec", "building", "label", "geometry"]]

        building_gdf = gpd.sjoin_nearest(
            building_gdf, street_gdf, "left", 100, distance_col="b_closest_street"  # type: ignore
        )

        tessel_gdf = tessel_gdf.reset_index()
        building_gdf = building_gdf.drop_duplicates("bID_kec").drop(columns="index_right")
        street_gdf = street_gdf[["sID", "geometry_type"]]

        logging.info(f"{name}: Create contiguity spatial weights...")
        # Spatial Weights : Contiguity and DistanceBand
        ## Contiguity - Cocok untuk neighborhood
        sw1 = libpysal.weights.contiguity.Queen.from_dataframe(
            tessel_gdf, ids="bID_kec", silence_warnings=True
        )
        sw3 = libpysal.weights.higher_order(sw1, k=3, lower_order=True, silence_warnings=True)

        logging.info(f"{name}: Calculate features!")
        building_gdf, tessel_gdf, street_gdf = dimension(
            building_gdf, tessel_gdf, street_gdf, sw3
        )
        building_gdf, tessel_gdf, street_gdf = shape(building_gdf, tessel_gdf, street_gdf)
        building_gdf, tessel_gdf, street_gdf = spatial_distribution(
            building_gdf, tessel_gdf, street_gdf, sw1, sw3
        )
        building_gdf, tessel_gdf, street_gdf = intensity(building_gdf, tessel_gdf, street_gdf)
        building_gdf, nodes_gdf, street_gdf = graph(building_gdf, street_gdf)

        building_gdf = building_gdf.merge(
            tessel_gdf.drop(columns="geometry"), on="bID_kec", how="left"
        )
        building_gdf = building_gdf.merge(
            street_gdf.drop(columns="geometry"), on="sID", how="left"  # type: ignore
        )
        building_gdf = building_gdf.merge(nodes_gdf.drop(columns="geometry"), on="nodeID")  # type: ignore

        building_gdf.to_csv(os.path.join(folder_out, f"{name}_building.csv"), index=False)
        tessel_gdf.to_csv(os.path.join(folder_out, f"{name}_tessel.csv"), index=False)
        street_gdf.to_csv(os.path.join(folder_out, f"{name}_street.csv"), index=False)  # type: ignore
        nodes_gdf.to_csv(os.path.join(folder_out, f"{name}_nodes.csv"), index=False)  # type: ignore

        df_list.append(building_gdf)

        del building_gdf, tessel_gdf, street_gdf, nodes_gdf
        gc.collect()

    except Exception as e:
        logging.warning(f"{name}: Error - {e}")
        continue

    logging.info(f"{name} done!")
