import os

del os.environ["PROJ_LIB"]  # Ada conflict antara PROJ dari venv dengan PROJ dari PostgreSQL
import geopandas as gpd
import momepy as mm
import glob
import logging
import libpysal
import warnings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Paths setup
building_path = "Data Collection/building_clean/validation"
street_path = r"Data Collection\street_clean\validation"
tessel_path = "Data Processing/tessellation/validation"
output_path = "Data Processing/Calculated_Building/validation"

os.makedirs(output_path, exist_ok=True)


kecamatan = [
    "Cilandak",
    "Jagakarsa",
    "Kebayoran Baru",
    "Kebayoran Lama",
    "Mampang Prapatan",
    "Pancoran",
    "Pasar Minggu",
    "Pesanggrahan",
    "Setiabudi",
    "Tebet",
]

warnings.filterwarnings("ignore")
# Attributes to be dropped later
"""
Orientation - Karena rawan ngebawa spatial autocorrelation - malah bikin bias
"""


def dimension(building, tessel, street, sw1, sw3):
    logging.info("Dimension...")
    building_gdf = building
    tessel_gdf = tessel[["bID", "geometry"]]
    street_gdf = street[["sID", "geometry"]]

    building_gdf["b_area"] = building_gdf.area
    building_gdf["b_perimeter"] = mm.Perimeter(building_gdf).series
    building_gdf["b_lal"] = mm.LongestAxisLength(building_gdf).series

    tessel_gdf["t_area"] = tessel_gdf.area
    tessel_gdf["t_perimeter"] = mm.Perimeter(tessel_gdf).series
    tessel_gdf["t_cov_area"] = mm.CoveredArea(tessel_gdf, sw3, "bID", False).series

    street_gdf["s_sum_length"] = mm.SegmentsLength(street_gdf, verbose=False).sum
    street_gdf["s_length"] = street_gdf.length

    building_gdf = building_gdf.merge(
        tessel_gdf.drop(columns="geometry"), on="bID", how="left"
    )
    building_gdf = building_gdf.merge(
        street_gdf.drop(columns="geometry"), on="sID", how="left"
    )

    """
    We can add another averaged character to learn how the average feature around
    the building with mm.AverageCharacter
    """
    return building_gdf


def shape(building, tessel, street):
    logging.info("Shape...")
    building_gdf = building
    tessel_gdf = tessel[["bID", "geometry"]]
    street_gdf = street[["sID", "geometry"]]

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

    building_gdf = building_gdf.merge(
        tessel_gdf.drop(columns="geometry"), on="bID", how="left"
    )
    building_gdf = building_gdf.merge(
        street_gdf.drop(columns="geometry"), on="sID", how="left"
    )

    return building_gdf


def spatial_distribution(building, tessel, street, sw1=None, sw3=None):

    building_gdf = building
    tessel_gdf = tessel[["bID", "geometry"]]
    street_gdf = street[["sID", "geometry"]]

    ## Distance Band - Cocok untuk hubungan spasial yang bersifat continues
    print("Adjacency")
    building_gdf["b_adjacency"] = mm.BuildingAdjacency(
        building_gdf, sw3, "bID", verbose=False
    ).series  # Using building spatial weight and tessel sw higher

    print("dis1")
    dist = libpysal.weights.DistanceBand.from_dataframe(building_gdf, 50, ids="bID")
    building_gdf["b_neighbor_50"] = mm.Neighbors(
        building_gdf, dist, "bID", verbose=False
    ).series

    print("dis2")
    dist = libpysal.weights.DistanceBand.from_dataframe(building_gdf, 150, ids="bID")
    building_gdf["b_neighbor_150"] = mm.Neighbors(
        building_gdf, dist, "bID", verbose=False
    ).series

    print("dis3")
    dist = libpysal.weights.DistanceBand.from_dataframe(building_gdf, 300, ids="bID")
    building_gdf["b_neighbor_300"] = mm.Neighbors(
        building_gdf, dist, "bID", verbose=False
    ).series

    # Either use mean interbuilding distance or neighbour distance
    print("neigh_dis")
    building_gdf["b_neigh_dis"] = mm.NeighborDistance(building_gdf, sw1, "bID", False).series

    print("b_orient")
    building_gdf["b_orientation"] = mm.Orientation(building_gdf, False).series

    print("b_align")
    building_gdf["b_alignment"] = mm.Alignment(
        building_gdf, sw1, "bID", "b_orientation", False
    ).series

    print("t_orinent")
    tessel_gdf["t_orientation"] = mm.Orientation(tessel_gdf, False).series

    print("t_align")
    tessel_gdf["t_alignment"] = mm.CellAlignment(
        building_gdf, tessel_gdf, "b_orientation", "t_orientation", "bID", "bID"
    ).series

    print("s_align")
    street_gdf["s_alignment"] = mm.StreetAlignment(
        building_gdf, street_gdf, "b_orientation", network_id="sID"
    ).series

    building_gdf = building_gdf.merge(
        tessel_gdf.drop(columns="geometry"), on="bID", how="left"
    )
    building_gdf = building_gdf.merge(
        street_gdf.drop(columns="geometry"), on="sID", how="left"
    )

    return building_gdf.drop(columns=["b_orientation", "t_orientation"])


def intensity(building, tessel, street):
    logging.info("Intensity...")
    building_gdf = building
    tessel_gdf = tessel[["bID", "geometry"]]
    street_gdf = street[["sID", "geometry"]]

    tessel_gdf["t_area"] = tessel_gdf.area

    building_gdf["b_area_ratio"] = mm.AreaRatio(
        tessel_gdf, building_gdf, "t_area", "b_area", "bID"
    ).series

    street_gdf["s_reached"] = mm.Reached(
        street_gdf, building_gdf, "sID", "sID", verbose=False
    ).series

    building_gdf = building_gdf.merge(street_gdf.drop(columns="geometry"), on="sID")

    return building_gdf


def diversity(building):
    return


def graph(building, street):
    logging.info("Graph...")
    building_gdf = building
    street_gdf = street[["sID", "geometry"]]

    graph = mm.gdf_to_nx(street_gdf)
    graph = mm.node_degree(graph, "n_degree")
    logging.info("Betweenness...")
    # Seberapa terhubung sebuah titik di jaringan jalan perkotaan
    graph = mm.betweenness_centrality(
        graph, "n_betwenness", radius=500, distance="mm_len", verbose=False
    )
    logging.info("Closeness...")
    # Seberapa terpusat sebuah titik di jaringan jalan perkotaan
    graph = mm.closeness_centrality(
        graph, name="n_closeness", radius=500, distance="mm_len", verbose=False
    )
    # logging.info("Straightness...")
    # graph = mm.straightness_centrality(
    #     graph, "mm_len", False, "n_straightness", 500, verbose=False
    # )
    logging.info("Meshedness...")
    graph = mm.meshedness(graph, name="n_meshed", radius=500, distance="mm_len", verbose=False)

    nodes_gdf, street_gdf = mm.nx_to_gdf(graph)

    building_gdf["nodeID"] = mm.get_node_id(
        building_gdf, nodes_gdf, street_gdf, "nodeID", "sID", verbose=False
    )

    building_gdf = building_gdf.merge(nodes_gdf.drop(columns="geometry"), on="nodeID")
    building_gdf = building_gdf.merge(street_gdf.drop(columns="geometry"), on="sID")

    return building_gdf


# ----------------------------------------------------------------


def calculate(kec: str):
    logging.info(f"{kec} Start!")
    # Calls specific kecamatan
    building_gdf = glob.glob(os.path.join(building_path, f"{kec}_1.geojson"))[0]
    tess_files_shp = glob.glob(os.path.join(tessel_path, f"*{kec}_tess.shp"))
    tess_files_geojson = glob.glob(os.path.join(tessel_path, f"*{kec}_tess.geojson"))
    street_gdf = glob.glob(os.path.join(street_path, f"*{kec}_drive_1.geojson"))[0]

    if tess_files_shp:
        tess_gdf = tess_files_shp[0]  # Prioritize .shp
    elif tess_files_geojson:
        tess_gdf = tess_files_geojson[0]  # Use .geojson if .shp is not found
    else:
        logging.error(
            f"No tessellation file found for {kec} in either .shp or .geojson format."
        )
        return  # Exit the function if no file is found

    # Opening the file (Building, Tessel)
    logging.info("Opening file...")
    tessel_gdf = gpd.read_file(tess_gdf)
    buildings_gdf = gpd.read_file(building_gdf)
    street_gdf = gpd.read_file(street_gdf)

    # Special for Kebayoran Lama
    if kec == "Kebayoran Lama":
        buildings_gdf = buildings_gdf[buildings_gdf.geometry.is_valid]

    if buildings_gdf.crs != "EPSG:32748":
        buildings_gdf.set_crs("EPSG:32748", allow_override=True)
    if tessel_gdf.crs != "EPSG:32748":
        tessel_gdf.set_crs("EPSG:32748", allow_override=True)
    if street_gdf.crs != "EPSG:32748":
        street_gdf.set_crs("EPSG:32748", allow_override=True)

    # Connect building to nearest street and add 'Highways' information to building gdf
    buildings_gdf = gpd.sjoin_nearest(
        buildings_gdf, street_gdf, "left", 100, distance_col="b_closest_street"
    )
    buildings_gdf = buildings_gdf.drop_duplicates("bID").drop(columns="index_right")
    street_gdf = street_gdf[["sID", "geometry"]]  # drop highways

    logging.info("Create contiguity spatial weights...")
    # Spatial Weights : Contiguity and DistanceBand
    ## Contiguity - Cocok untuk neighborhood
    tessel_gdf["bID"] = tessel_gdf["bID"].astype(int)
    sw1 = libpysal.weights.contiguity.Queen.from_dataframe(
        tessel_gdf, ids="bID", silence_warnings=True
    )
    sw3 = libpysal.weights.higher_order(sw1, k=3, lower_order=True, silence_warnings=True)

    # Calculate the Matrices
    logging.info("Calculate features!")
    # buildings_gdf = dimension(buildings_gdf, tessel_gdf, street_gdf, sw1, sw3)
    # buildings_gdf = shape(buildings_gdf, tessel_gdf, street_gdf)
    buildings_gdf = spatial_distribution(buildings_gdf, tessel_gdf, street_gdf)
    # buildings_gdf = intensity(buildings_gdf, tessel_gdf, street_gdf)
    # buildings_gdf = graph(buildings_gdf, street_gdf)
    # buildings_gdf = diversity()

    return buildings_gdf


for kec in ["Cilincing"]:
    # try:
    df = calculate(kec)
    # except Exception as e:
    #     logging.error(f"{kec} Failed! : {e}")
    #     continue
    path = os.path.join(output_path, f"{kec}_calculated.geojson")
    df.to_file(path, driver="GeoJSON")
