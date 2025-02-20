import os

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define the local path and URL
csv_file_path = "les-arbres.csv"
csv_url = "https://opendata.paris.fr/explore/dataset/les-arbres/download/?format=csv"


def download_file_if_not_exists(local_path, url):
    """
    Check if the file exists locally; if not, download it from the specified URL.

    Parameters:
    local_path (str): The local file path where the file should be saved.
    url (str): The URL to download the file from.
    """
    if not os.path.exists(local_path):
        print(f"File not found at {local_path}. Downloading from {url}...")
        response = requests.get(url)
        response.raise_for_status()  # Ensure the request was successful
        with open(local_path, "wb") as file:
            file.write(response.content)
        print("Download complete.")
    else:
        print(f"File already exists at {local_path}.")


# Ensure the CSV file is present
download_file_if_not_exists(csv_file_path, csv_url)

# Initialize SparkSession
spark = SparkSession.builder.appName("ParisTreesAnalysis").getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True, sep=";")

# Show the first five rows
df.show(5)

# Print the schema of the DataFrame
df.printSchema()

# Select specific columns
df.select("idbase", "libellefrancais").show(5)

# Get summary statistics
df.describe().show()

# Filter rows based on a condition
df.filter(df["hauteurenm"] > 15).show(5)

# Count the number of rows
row_count = df.count()
print(f"Total number of rows: {row_count}")

# Stop the SparkSession
spark.stop()
