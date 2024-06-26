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
#### a. Data Preprocessing
At this step, I preprocess multiple data such as building, streets, also landuse data using multiple script for each dataset. I also add utils folder for any misc processing
#### b. Create Morphometric Elements
Before we can calculate the morphometric elements, we need to create a tessellation cell for every buildings.
#### c. Calculate The Building Morphometric Values
We calculate the morphometric values using Momepy library. Here we calculate all values that represent the building and its surrounding buildings.

### 3. Create the Classification Models

#### a. Create Initial Model
At this step, we use XGBoost algorithm for the classification of the building use. At first we use initial value for every hyperparameter.
#### b. Hyperparameters Tuning
Then, we try to optimize the hyperparameters with a spatial cross validation to make sure that the model can handle unseen data from another region.
#### c. Model Testing
After we get the ideal hyperparameters, we try to test the model using Jakarta Utara's buildings. 

Ati this step, we conduct a several experiments.

1. With no local data
2. With 2,5% local data
3. With 5% local data
4. With 10% local data

## Dependencies
- OSMnx
- Momepy
- XGBoost
- Geopandas
- Pandas
