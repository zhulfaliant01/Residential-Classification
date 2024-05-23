# Residential Building Classification with XGBoost Based on Morphological Character and OpenStreetMap Atributes (OSM)

## Description
This is a my final project for my bachelor of Geodetic Engineering from Gadjah Mada Univeristy. With this project, we can identify residential building from an OpenStreetMap building dataset using only the building geometry and its OSM atributes.

The scope of this project is to use the OpenStreetMap dataset to create a machine learning model with XGBoost in South Jakarta.

## Workflow
### 1. Prepare the Dataset
Dataset that I use in this project is from OpenStreetMap. I use OSMnx to download the building footprint and street network data. Because of the size and the largeness of the area, I batch the downloading for every subdistrict in South Jakarta. After download the data, we merge all of the data and get around 200k row of building footprint in South Jakarta.

Other than the building footprint and street network, we also need the landuse dataset from Jakarta Satu Website. We also merge all the landuse data into one geojson file.

Problems: Some of the 'kelurahan' have the same name as the 'kecamatan', so we need to query each 'kelurahan' in some 'kecamatan'

### 2. Process the Dataset
In this step, there will be a multiple of process we need to do.
#### a. Quality Control
At
#### b. Create Morphometric Elements

#### c. Create Morphometric Characters

## Dependencies
- OSMnx
- Momepy
- XGBoost
- Geopandas
- Pandas
- Dask-Geopandas