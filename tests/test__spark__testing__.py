import os

import pytest
import requests
from pyspark.sql import SparkSession


# Function to check and download the dataset if not present
def download_dataset(url, local_path):
    if not os.path.exists(local_path):
        response = requests.get(url)
        with open(local_path, "wb") as f:
            f.write(response.content)


# Define the dataset URL and local path
dataset_url = (
    "https://opendata.paris.fr/explore/dataset/les-arbres/download/?format=csv"
)
local_csv_path = "les-arbres.csv"

# Download the dataset before running tests
download_dataset(dataset_url, local_csv_path)


# Load the dataset into a DataFrame
@pytest.fixture(scope="session")
def df(spark):
    return spark.read.csv(local_csv_path, header=True, inferSchema=True, sep=";")


# Test to validate DataFrame schema
def test_schema(df):
    expected_columns = {
        "idbase",
        "typeemplacement",
        "domanialite",
        "arrondissement",
        "complementadresse",
        "numero",
        "libellefrancais",
        "hauteurenm",
        "circonferenceencm",
        "geo_point_2d",
    }
    assert expected_columns.issubset(set(df.columns))


# Test to check for null values in critical columns
def test_null_values(df):
    critical_columns = ["idbase", "libellefrancais"]
    for col in critical_columns:
        assert df.filter(df[col].isNull()).count() == 0


# Test to ensure HAUTEUR and CIRCONFERENCE have positive values
def test_positive_values(df):
    assert df.filter(df["hauteurenm"] <= 0).count() == 0
    assert df.filter(df["circonferenceencm"] <= 0).count() == 0


# Test to validate geo_point_2d column contains valid coordinates
def test_geolocation(df):
    from pyspark.sql.functions import col, split

    df = df.withColumn(
        "latitude", split(col("geo_point_2d"), ",").getItem(0).cast("float")
    )
    df = df.withColumn(
        "longitude", split(col("geo_point_2d"), ",").getItem(1).cast("float")
    )
    assert (
        df.filter((col("latitude").isNull()) | (col("longitude").isNull())).count() == 0
    )
    assert df.filter((col("latitude") < -90) | (col("latitude") > 90)).count() == 0
    assert df.filter((col("longitude") < -180) | (col("longitude") > 180)).count() == 0
