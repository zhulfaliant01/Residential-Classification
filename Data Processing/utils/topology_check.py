import geopandas as gpd
from pyproj import CRS
from shapely.geometry import LineString
import logging
import glob
import os
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

del os.environ["PROJ_LIB"]


def check_overlap(gdf, id_col=None, threshold: float = 0):

    try:
        crs = CRS(gdf.crs)
        if crs.is_geographic:
            logging.warning(
                "GeoDataFrame is in a geographic CRS. Consider reprojecting to a projected CRS for accurate area measurements."
            )

        if id_col == None:
            gdf["id"] = range(1, len(gdf) + 1)
            id_col = "id"

        gdf["temp_index"] = gdf.index
        spatial_index = gdf.sindex
        data_overlaps = []

        for index, row in gdf.iterrows():
            try:
                possible_matches_index = list(spatial_index.intersection(row.geometry.bounds))
            except Exception as e:
                logging.warning(f"Error when looking for intersection : {e}")
                continue
            possible_matches = gdf.iloc[possible_matches_index].copy()
            possible_overlaps = possible_matches[
                possible_matches.geometry.overlaps(row.geometry)
            ]

            for _, possible_overlap_row in possible_overlaps.iterrows():
                if possible_overlap_row[id_col] == row[id_col]:
                    continue

                intersection = gpd.overlay(
                    gdf.loc[[index]],
                    gdf.loc[[possible_overlap_row["temp_index"]]],
                    how="intersection",
                )
                intersection["area"] = intersection.geometry.area

                if intersection["area"].iloc[0] >= threshold:
                    sorted_ids = sorted([row[id_col], possible_overlap_row[id_col]])
                    if sorted_ids not in data_overlaps:
                        data_overlaps.append(
                            sorted_ids
                            + [
                                intersection.geometry.iloc[0],
                                intersection["area"].iloc[0],
                            ]
                        )

        if data_overlaps is not None:
            overlaps_gdf = gpd.GeoDataFrame(
                data_overlaps,
                columns=[f"{id_col}_1", f"{id_col}_2", "geometry", "area"],
                crs=gdf.crs,
            )  # type:ignore
        else:
            return None

        del gdf["temp_index"]
        overlaps_gdf.drop_duplicates(inplace=True, ignore_index=True)
        return overlaps_gdf

    except Exception as e:
        logging.error("Failed in check_overlap: %s", e)
        raise


def check_gap(gdf, id_col):
    """
    Identifies gaps within the unified geometry of the input GeoDataFrame and finds features
    touching each gap.

    Parameters:
    - gdf: Input GeoDataFrame with geometries.
    - id_col: Column name that uniquely identifies each feature in gdf.

    Returns:
    - GeoDataFrame containing the identified gaps and the IDs of features touching each gap.
    """
    try:
        if id_col == None:
            gdf["id"] = range(1, len(gdf) + 1)
            id_col = "id"

        gdf_copy = gdf.copy()
        gdf_copy["diss_id"] = 1
        data_temp_diss = gdf_copy.dissolve(by="diss_id")

        if not data_temp_diss.interiors.values.tolist():
            logging.warning(
                "No interiors found. The provided GeoDataFrame might not have gaps."
            )
            return gpd.GeoDataFrame(
                columns=[f"{id_col}_1", f"{id_col}_2", "geometry", "area"], crs=gdf.crs
            )  # type:ignore

        interiors = data_temp_diss.interiors.values.tolist()[0]
        gap_list = [LineString(i) for i in interiors]
        data_gaps = gpd.GeoDataFrame(geometry=gap_list, crs=gdf.crs)  # type:ignore
        gdf_sindex = gdf_copy.sindex

        data_gaps["feature_touches"] = data_gaps.geometry.apply(
            lambda gap_geom: gdf_copy.iloc[
                list(gdf_sindex.query(gap_geom, predicate="touches"))
            ][id_col].tolist()
        )

        data_gaps.drop_duplicates(inplace=True, ignore_index=True)
        data_gaps.dissolve(by=[f"{id_col}_1", f"{id_col}_2"], as_index=False)
        return data_gaps

    except Exception as e:
        logging.error("Failed in check_gap: %s", e)
        raise


def check_containment(gdf, id_col, min_area: float = 0):
    """
    This function checks for geometries in a GeoDataFrame that are completely contained
    within other geometries and identifies those that meet a specified minimum area criteria.

    Parameters:
    - gdf: A GeoDataFrame containing the geometries to be checked.
    - id_col: The name of the column that uniquely identifies each geometry.
    - min_area: The minimum area of contained geometries to consider; geometries below this size are ignored.

    Returns:
    - A GeoDataFrame containing pairs of geometries where one is contained within the other,
      exceeding the specified minimum area threshold. Each row in the returned GeoDataFrame
      represents a unique pair of geometries, including the geometry of the contained feature and its area.
    """
    try:
        crs = CRS(gdf.crs)
        if crs.is_geographic:
            logging.warning(
                "GeoDataFrame is in a geographic CRS. Consider reprojecting to a projected CRS for accurate area measurements."
            )

        if id_col == None:
            gdf["id"] = range(1, len(gdf) + 1)
            id_col = "id"

        gdf["temp_index"] = gdf.index
        spatial_index = gdf.sindex
        data_containment = []

        for _, row in gdf.iterrows():
            possible_matches_index = list(
                spatial_index.query(row.geometry, predicate="contains")
            )
            possible_matches = gdf.iloc[possible_matches_index].copy()

            for _, possible_contained_row in possible_matches.iterrows():
                drop = 0
                if possible_contained_row[id_col] == row[id_col]:
                    continue

                if row.geometry.contains(possible_contained_row.geometry):
                    area = possible_contained_row.geometry.area
                    if area == row.geometry.area:
                        drop = 1
                    if area >= min_area:
                        sorted_ids = sorted([row[id_col], possible_contained_row[id_col]])
                        if sorted_ids not in data_containment:
                            data_containment.append(
                                sorted_ids + [possible_contained_row.geometry, area, drop]
                            )

        if data_containment:
            containment_gdf = gpd.GeoDataFrame(
                data_containment,
                columns=[f"{id_col}_1", f"{id_col}_2", "geometry", "area", "drop"],
                crs=gdf.crs,
            )  # type:ignore
        else:
            print("None found")
            return None

        del gdf["temp_index"]
        containment_gdf.drop_duplicates(
            subset=[f"{id_col}_1", f"{id_col}_2", "area"], inplace=True, ignore_index=True
        )
        return containment_gdf
    except Exception as e:
        logging.error("Failed in check_containment: %s", e)
        raise


def _main():
    """
    For internal use only
    """

    def run_function(file):
        logging.info("Starting %s!", file)
        gdf = gpd.read_file(file)
        crs = "EPSG:32748"

        if gdf.crs != crs:
            gdf = gdf.to_crs(crs)

        overlaps_gdf = check_overlap(gdf, "bID", 0.5)  # type:ignore
        if len(overlaps_gdf) > 0:  # type: ignore
            print(len(overlaps_gdf))  # type: ignore
            overlaps_gdf.to_file(  # type: ignore
                file.replace("_1.geojson", "_overlaps.geojson"), driver="GeoJSON"
            )
        else:
            logging.warning("No overlaps found in %s", file)

        containment_gdf = check_containment(gdf, "bID", 0.5)  # type:ignore
        if len(containment_gdf) > 0:  # type: ignore
            print(len(containment_gdf))  # type: ignore
            containment_gdf.to_file(  # type: ignore
                file.replace("_1.geojson", "_containment.geojson"), driver="GeoJSON"
            )
        else:
            logging.warning("No containments found in %s", file)

        logging.info("Succeed : %s", file)

    try:
        files_path = r"Data Collection\building_clean\test"
        files = glob.glob(os.path.join(files_path, "*.geojson"))
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(run_function, file) for file in files]

    except Exception as e:
        logging.error("Failed in main: %s", e)


if __name__ == "__main__":
    _main()
