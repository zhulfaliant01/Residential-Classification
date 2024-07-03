import os
import geopandas as gpd
import pandas as pd
import momepy as mm
import glob
import logging
import libpysal
import json
import re

from utils import (
    read_csv_to_wkt,
    check_and_set_crs,
    del_gc,
    mm_std_character,
    mm_total_area,
    street_centrality_value,
    find_street_fr_building,
    mm_street_character,
    mm_count_intersections,
    check_correct_multipart,
)
import traceback

# del os.environ["PROJ_LIB"]  # Ada conflict antara PROJ dari venv dengan PROJ dari PostgreSQL

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

dataset_type = (
    "test"  # Sebelum hitung training, data jalannya perlu dipecah dulu
)

# Load configuration from JSON file
with open(r"config.json", "r") as config_file:
    config = json.load(config_file)

# Set input and output folders from the configuration
building_in = config.get(f"morphometric_{dataset_type}_paths")[
    "input_building"
]
street_in = config.get(f"morphometric_{dataset_type}_paths")["input_street"]
tessel_in = config.get(f"morphometric_{dataset_type}_paths")["input_tessel"]
folder_out = config.get(f"morphometric_{dataset_type}_paths")["output"]
emergency_folder = r"D:\backup skripsi\backup processing"

os.makedirs(folder_out, exist_ok=True)

crs = config.get("crs")


def ft_building_tess_geom(building, tessel):
    logging.info(f"{name}: Building geometry Feature Group...")
    building_gdf = building
    tessel_gdf = tessel

    logging.info(f"{name}: Building...")
    building_gdf["b_area"] = building_gdf.area
    building_gdf["b_perimeter"] = mm.Perimeter(building_gdf).series
    building_gdf["b_convexity"] = mm.Convexity(building_gdf).series
    building_gdf["b_circular_comp"] = mm.CircularCompactness(
        building_gdf
    ).series
    ccd = mm.CentroidCorners(building_gdf, verbose=False)
    building_gdf["b_ccd_means"] = ccd.mean
    building_gdf["b_ccd_std"] = ccd.std
    del_gc(ccd)
    building_gdf["b_corners"] = mm.Corners(building_gdf, verbose=False).series
    building_gdf["b_elong"] = mm.Elongation(building_gdf).series
    building_gdf["b_lal"] = mm.LongestAxisLength(building_gdf).series
    building_gdf["b_eri"] = mm.EquivalentRectangularIndex(
        building_gdf, "b_area", "b_perimeter"
    ).series
    building_gdf["b_orientation"] = mm.Orientation(
        building_gdf, verbose=False
    ).series
    # ----------------------------------------------------------------
    logging.info(f"{name}: Tessellation...")
    tessel_gdf["t_area"] = tessel_gdf.area
    tessel_gdf["t_perimeter"] = mm.Perimeter(tessel_gdf).series
    tessel_gdf["t_convexity"] = mm.Convexity(tessel_gdf).series
    tessel_gdf["t_circular_comp"] = mm.CircularCompactness(tessel_gdf).series
    tessel_gdf["t_elong"] = mm.Elongation(tessel_gdf).series
    tessel_gdf["t_lal"] = mm.LongestAxisLength(tessel_gdf).series
    tessel_gdf["t_eri"] = mm.EquivalentRectangularIndex(
        tessel_gdf, "t_area", "t_perimeter"
    ).series
    tessel_gdf["t_orientation"] = mm.Orientation(
        tessel_gdf, verbose=False
    ).series
    # ----------------------------------------------------------------
    building_gdf["b_area_ratio"] = mm.AreaRatio(
        tessel_gdf, building_gdf, "t_area", "b_area", "bID_kec"
    ).series

    return building_gdf, tessel_gdf


def ft_building_tess_neigh(building, tessel):
    logging.info(f"{name}: Building Immediate Neighbor...")
    building_gdf = building
    tessel_gdf = tessel

    logging.info(f"{name}: SW1...")
    sw1 = libpysal.weights.contiguity.Queen.from_dataframe(
        tessel_gdf, ids="bID_kec", silence_warnings=True
    )

    building_gdf["b_neigh_dis"] = mm.NeighborDistance(
        building_gdf, sw1, "bID_kec", False
    ).series
    building_gdf["b_alignment"] = mm.Alignment(
        building_gdf, sw1, "bID_kec", "b_orientation", False
    ).series
    tessel_gdf["t_alignment"] = mm.CellAlignment(
        building_gdf,
        tessel_gdf,
        "b_orientation",
        "t_orientation",
        "bID_kec",
        "bID_kec",
    ).series

    logging.info(f"{name}: SW3...")
    sw3 = libpysal.weights.higher_order(
        sw1, k=3, lower_order=True, silence_warnings=True
    )

    del_gc(sw1)

    building_gdf["b_adjacency"] = mm.BuildingAdjacency(
        building_gdf, sw3, "bID_kec", verbose=False
    ).series
    tessel_gdf["t_cov_area"] = mm.CoveredArea(
        tessel_gdf, sw3, "bID_kec", False
    ).series

    del_gc(sw3)

    return building_gdf, tessel_gdf


def ft_building_tess_dist(building, tessel, dist):
    logging.info(f"Building Dist {dist} start...")
    building_gdf = building
    tessel_gdf = tessel

    logging.info(f"{name}: Create sw ({dist}m)...")
    sw_dist = libpysal.weights.DistanceBand.from_dataframe(
        building_gdf, dist, ids="bID_kec"
    )
    building_gdf[f"b_neighbor_{dist}"] = mm.Neighbors(
        building_gdf, sw_dist, "bID_kec", verbose=False
    ).series

    b_values = [
        "b_area",
        "b_perimeter",
        "b_convexity",
        "b_circular_comp",
        "b_elong",
        "b_lal",
        "b_eri",
    ]

    logging.info(f"{name}: Calculate building ({dist}m)...")  # ~ 10 menit
    new_gdf = gpd.GeoDataFrame(index=building_gdf.index)  # type: ignore
    # Loop through each building characteristic
    for value in b_values:
        character = mm.AverageCharacter(
            building_gdf, value, sw_dist, "bID_kec", verbose=False
        )
        new_gdf[f"b_avg_{value[2:]}_{dist}"] = character.mean
        new_gdf[f"b_median_{value[2:]}_{dist}"] = character.median
        new_gdf[f"b_std_{value[2:]}_{dist}"] = mm_std_character(
            building_gdf, sw_dist, value, "bID_kec"
        )

    # Concatenate the new columns to the original building_gdf
    building_gdf = pd.concat([building_gdf, new_gdf], axis=1)

    del_gc(new_gdf)

    t_values = [
        "t_area",
        "t_perimeter",
        "t_convexity",
        "t_circular_comp",
        "t_elong",
        "t_lal",
        "t_eri",
    ]
    logging.info(f"{name}: Calculate tessellation ({dist}m)...")
    new_gdf = gpd.GeoDataFrame(index=tessel_gdf.index)
    for value in t_values:
        character = mm.AverageCharacter(
            tessel_gdf, value, sw_dist, "bID_kec", verbose=False
        )
        new_gdf[f"t_avg_{value[2:]}_{dist}"] = character.mean
        new_gdf[f"t_median_{value[2:]}_{dist}"] = character.median
        new_gdf[f"t_std_{value[2:]}_{dist}"] = mm_std_character(
            tessel_gdf, sw_dist, value, "bID_kec"
        )
    tessel_gdf = pd.concat([tessel_gdf, new_gdf], axis=1)

    del_gc(new_gdf)

    building_gdf[f"b_total_area_{dist}"] = mm_total_area(
        building_gdf, sw_dist, "bID_kec"
    )

    del_gc(sw_dist)

    return building_gdf, tessel_gdf


def ft_closest_street(building, street):
    logging.info(f"{name}: Closest street...")
    building_gdf = building
    street_gdf = street

    profile = mm.StreetProfile(street_gdf, building_gdf)
    street_gdf["s_width"] = profile.w
    street_gdf["s_width_def"] = profile.wd
    street_gdf["s_openness"] = profile.o
    street_gdf["s_sum_length"] = mm.SegmentsLength(
        street_gdf, verbose=False
    ).sum
    street_gdf["s_alignment"] = mm.StreetAlignment(
        building_gdf, street_gdf, "b_orientation", network_id="sID"
    ).series

    del_gc(profile)

    logging.info(f"{name}: Create graph...")
    graph = mm.gdf_to_nx(street_gdf, length="s_length")
    graph = mm.node_degree(graph, "n_degree")

    logging.info(
        f"{name}: Betweenness Centrality (500m)..."
    )  # ~ 5 menit per centrality
    graph = mm.betweenness_centrality(
        graph,
        name="n_betweenness_500",
        radius=500,
        weight="s_length",
        distance="s_length",
        verbose=False,
    )

    logging.info(f"{name}: Closeness Centrality (500m)...")
    graph = mm.closeness_centrality(
        graph,
        name="n_closeness_500",
        radius=500,
        weight="s_length",
        distance="s_length",
        verbose=False,
    )

    logging.info(f"{name}: Straightness Centrality (500m)...")
    graph = mm.straightness_centrality(
        graph,
        name="n_straightness_500",
        weight="s_length",
        distance="s_length",
        radius=500,
        verbose=False,
    )

    logging.info(f"{name}: Meshedness (500m)...")
    graph = mm.meshedness(
        graph,
        name="n_meshedness_500",
        radius=500,
        distance="s_length",
        verbose=False,
    )

    logging.info(f"{name}: Betweenness Centrality (global)...")  # ~ 7 menit
    graph = mm.betweenness_centrality(
        graph,
        name="n_betweenness_global",
        weight="s_length",
        distance="s_length",
        verbose=False,
    )

    logging.info(f"{name}: Closeness Centrality (global)...")
    graph = mm.closeness_centrality(
        graph,
        name="n_closeness_global",
        weight="s_length",
        distance="s_length",
        verbose=False,
    )

    logging.info(f"{name}: Straightness Centrality (global)...")
    graph = mm.straightness_centrality(
        graph,
        name="n_straightness_global",
        weight="s_length",
        distance="s_length",
        verbose=False,
    )

    logging.info(f"{name}: Meshedness (global)...")
    graph = mm.meshedness(
        graph,
        name="n_meshedness_global",
        distance="s_length",
        verbose=False,
    )

    nodes_gdf, street_gdf = mm.nx_to_gdf(graph)  # type: ignore

    intersection = nodes_gdf[nodes_gdf.n_degree > 2].copy()  # type: ignore
    # closest intersection
    building_gdf = gpd.sjoin_nearest(
        building_gdf, intersection[["n_degree", "geometry"]], "left", max_distance=150, distance_col="b_closest_intersect_dist"  # type: ignore
    )

    logging.info(f"{name}: Calculate street centrality...")
    street_gdf["s_betweenness_500"] = street_centrality_value(street_gdf, nodes_gdf, "n_betweenness_500")  # type: ignore
    street_gdf["s_betweenness_global"] = street_centrality_value(street_gdf, nodes_gdf, "n_betweenness_global")  # type: ignore
    street_gdf["s_closeness_500"] = street_centrality_value(street_gdf, nodes_gdf, "n_closeness_500")  # type: ignore
    street_gdf["s_closeness_global"] = street_centrality_value(street_gdf, nodes_gdf, "n_closeness_global")  # type: ignore
    street_gdf["s_straightness_500"] = street_centrality_value(street_gdf, nodes_gdf, "n_straightness_500")  # type: ignore
    street_gdf["s_straightness_global"] = street_centrality_value(street_gdf, nodes_gdf, "n_straightness_global")  # type: ignore
    street_gdf["s_meshedness_500"] = street_centrality_value(street_gdf, nodes_gdf, "n_meshedness_500")  # type: ignore
    street_gdf["s_meshedness_global"] = street_centrality_value(street_gdf, nodes_gdf, "n_meshedness_global")  # type: ignore

    return building_gdf, nodes_gdf, street_gdf


def ft_distance_street(building, street, nodes, dist):
    logging.info(f"{name}: Distance Street ({dist}m)...")
    building_gdf = building
    street_gdf = street
    nodes_gdf = nodes

    # Can i use distance band on street?
    street_sw = find_street_fr_building(
        building_gdf, street_gdf, dist, "bID_kec", "sID"
    )
    print(street_sw)
    values = [
        "s_betweenness_500",
        "s_closeness_500",
        "s_straightness_500",
        "s_meshedness_500",
    ]

    logging.info(f"{name}: Calculate distance street values...")
    new_gdf = gpd.GeoDataFrame(index=building_gdf.index)
    for value in values:
        # Extract the necessary components of the value string
        value_suffix = value[2:]

        # Call the custom function
        result = mm_street_character(
            building_gdf,
            street_gdf,
            value,
            street_sw,
            "bID_kec",
            "sID",
            ["mean", "max"],
            dist,
            False,
        )  # dist for debug purposes

        # Add the results to the new_gdf
        (  # type: ignore
            new_gdf[f"s_av_{value_suffix}_street_{dist}"],
            new_gdf[f"s_max_{value_suffix}_street_{dist}"],
        ) = result  # type: ignore
    building_gdf = pd.concat([building_gdf, new_gdf], axis=1)

    del_gc(new_gdf)

    logging.info(f"{name}: Calculate width and length...")
    building_gdf[f"s_mean_width_street_{dist}"], building_gdf[f"s_std_width_street_{dist}"] = (  # type: ignore
        mm_street_character(
            building_gdf,
            street_gdf,
            "s_width",
            street_sw,
            "bID_kec",
            "sID",
            ["mean", "std"],
            dist,
            False,
        )
    )

    building_gdf[f"s_mean_length_street_{dist}"], building_gdf[f"s_total_length_street_{dist}"], building_gdf[f"s_std_length_street_{dist}"] = (  # type: ignore
        mm_street_character(
            building_gdf,
            street_gdf,
            "s_length",
            street_sw,
            "bID_kec",
            "sID",
            ["mean", "total", "std"],
            dist,
            False,
        )
    )

    del_gc(street_sw)

    # Find intersection nodes
    intersection = nodes_gdf[nodes_gdf.n_degree > 2]
    building_gdf[f"b_intersection_count_{dist}"] = mm_count_intersections(
        building_gdf, intersection, dist, "bID_kec"
    )

    del_gc(intersection)

    return building_gdf


building_files = glob.glob(
    os.path.join(building_in, "Tanjung Priok_final.csv")
)
street_files = glob.glob(os.path.join(street_in, "Tanjung Priok_drive.csv"))
tessel_files = glob.glob(os.path.join(tessel_in, "Tanjung Priok_tessel.csv"))

# # Temporary 4 Juni Only
# building_files.remove("Dataset\\2_building_clean\\training_clean\\Kebayoran Baru_final.csv")
# street_files.remove("Dataset\\2_street_clean\\training\\Kebayoran Baru_drive_2.csv")
# tessel_files.remove("Dataset\\4_tess\\training\\Kebayoran Baru_tessel.csv")

for building, street, tessel in zip(
    building_files, street_files, tessel_files
):

    name = re.search(r"\\([\w ]*)_final.csv", building).group(1)  # type: ignore
    try:
        logging.info(f"{name} start...")

        building_gdf = check_and_set_crs(read_csv_to_wkt(building), crs)
        street_gdf = check_and_set_crs(
            read_csv_to_wkt(street, index_col=0), crs
        )
        tessel_gdf = check_and_set_crs(read_csv_to_wkt(tessel), crs)

        building_gdf = building_gdf[["bID_kec", "building", "geometry"]]

        # Temporary Solution for multipolygon and multilinestring
        building_gdf = check_correct_multipart(
            building_gdf, "bID_kec", "MultiPolygon"
        )
        tessel_gdf = check_correct_multipart(
            tessel_gdf, "bID_kec", "MultiPolygon"
        )
        street_gdf = check_correct_multipart(
            street_gdf, "sID", "MultiLinestring"
        )

        building_gdf = gpd.sjoin_nearest(
            building_gdf, street_gdf[["sID", "geometry", "highway"]], "left", 150, distance_col="b_closest_street"  # type: ignore
        )

        tessel_gdf = tessel_gdf.reset_index()
        building_gdf = building_gdf.drop_duplicates("bID_kec").drop(
            columns="index_right"
        )
        street_gdf = street_gdf[["sID", "geometry"]]

        logging.info(f"{name}: Calculate features!")

        building_gdf, tessel_gdf = ft_building_tess_geom(
            building_gdf, tessel_gdf
        )  # ~ 5.13 menit
        building_gdf, tessel_gdf = ft_building_tess_neigh(
            building_gdf, tessel_gdf
        )  # ~ 2 menit
        building_gdf, tessel_gdf = ft_building_tess_dist(
            building_gdf, tessel_gdf, 50
        )  # ~ 20 menit
        building_gdf, tessel_gdf = ft_building_tess_dist(
            building_gdf, tessel_gdf, 150
        )  # ~ 25 menit
        building_gdf, tessel_gdf = ft_building_tess_dist(
            building_gdf, tessel_gdf, 300
        )
        building_gdf, nodes_gdf, street_gdf = ft_closest_street(
            building_gdf, street_gdf
        )
        building_gdf = ft_distance_street(
            building_gdf, street_gdf, nodes_gdf, 50
        )
        building_gdf = ft_distance_street(
            building_gdf, street_gdf, nodes_gdf, 150
        )
        building_gdf = ft_distance_street(
            building_gdf, street_gdf, nodes_gdf, 300
        )

        logging.info(f"{name}: Merging data to building...")
        building_gdf = building_gdf.merge(  # type: ignore
            tessel_gdf.drop(columns="geometry"), on="bID_kec", how="left"
        )
        building_gdf = building_gdf.merge(
            street_gdf.drop(columns="geometry"), on="sID", how="left"  # type: ignore
        )

        try:
            building_gdf.to_csv(
                os.path.join(folder_out, f"2_{name}_building.csv"), index=False
            )
        except Exception as e:
            logging.warning(f"{name}: Error - {e}")
            building_gdf.to_csv(
                os.path.join(emergency_folder, f"2_{name}_building.csv"),
                index=False,
            )
        del_gc(building_gdf)

        try:
            tessel_gdf.to_csv(
                os.path.join(folder_out, f"2_{name}_tessel.csv"), index=False
            )
        except Exception as e:
            logging.warning(f"{name}: Error - {e}")
            tessel_gdf.to_csv(
                os.path.join(emergency_folder, f"2_{name}_tessel.csv"),
                index=False,
            )
        del_gc(tessel_gdf)

        try:
            street_gdf.to_csv(os.path.join(folder_out, f"2_{name}_street.csv"), index=False)  # type: ignore
        except Exception as e:
            logging.warning(f"{name}: Error - {e}")
            street_gdf.to_csv(os.path.join(emergency_folder, f"2_{name}_street.csv"), index=False)  # type: ignore
        del_gc(street_gdf)

        try:
            nodes_gdf.to_csv(os.path.join(folder_out, f"2_{name}_nodes.csv"), index=False)  # type: ignore
        except Exception as e:
            logging.warning(f"{name}: Error - {e}")
            nodes_gdf.to_csv(os.path.join(emergency_folder, f"2_{name}_nodes.csv"), index=False)  # type: ignore
        del_gc(nodes_gdf)

        logging.info(f"{name} done!")
    except Exception as e:
        logging.warning(f"{name}: Error - {e}")
        traceback.print_exc()
        continue
