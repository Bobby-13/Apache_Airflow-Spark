# from pyspark.sql import SparkSession # type: ignore

# spark = SparkSession.builder \
#     .appName("spark_dataframe") \
#     .config("spark.authenticate", "false") \
#     .getOrCreate()

# df = spark.read.csv("${AIRFLOW_HOME}/jobs/data/loan.csv", header=True, inferSchema=True)

# df.show()

# spark.stop()

from pyspark.sql import SparkSession
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder \
    .appName("spark_dataframe") \
    .config("spark.authenticate", "false") \
    .getOrCreate()

# Path to the CSV file
# csv_file_path = "${AIRFLOW_HOME}/jobs/data/loan.csv"
csv_file_path = "jobs/data/loan.csv"
# Read CSV file into a Pandas DataFrame
pandas_df = pd.read_csv(csv_file_path)

# Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Show the first 10 rows of the Spark DataFrame
spark_df.show(10)

# Stop the SparkSession
spark.stop()


# from pyspark.sql import SparkSession


# spark = SparkSession.builder \
#     .appName("CSV Example") \
#     .getOrCreate()
# data = [("John", 30), ("Alice", 35), ("Bob", 40)]
# df = spark.createDataFrame(data, ["Name", "Age"])

# df.write.csv("example.csv", header=True)
# df_read = spark.read.csv("example.csv", header=True, inferSchema=True)
# df_read.show()
# # df.show()
# spark.stop()
