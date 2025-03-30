from pyspark.sql import SparkSession
import os, json

def format_json(path_to_json) -> list[dict]:
    """
    Function to format JSON file into a dictionary
    Args:
        path_to_json (str): Path to JSON file
    Returns:
        dict: List of dictionaries. The key of the inner dictionary is the manufacturer and the value is a list of models.
    """
    output = []
    with open(path_to_json, "r") as f:
        data = json.load(f)
    for key, value in data.items():
        inner_dict = {}
        inner_dict["key"] = key
        inner_dict["value"] = value
        output.append(inner_dict)
    return output

def main():
    # Set paths
    path_to_warehouse = os.path.join(os.path.dirname(__file__), "..", "..", "spark-warehouse")
    path_to_json = os.path.join(os.path.dirname(__file__), "aeroplane_models.json")
    #Set database name
    if os.environ.get("ENV") != "prod":
        database_name = f"{os.environ.get('USER')}_{os.environ.get('ENV')}"
    else:
        database_name = "abstraction_to_s3"
    print(f"Path to Spark warehouse: {path_to_warehouse}")
    print(f"Path to JSON file: {path_to_json}")
    print(f"Database name: {database_name}")
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("load_json_into_sparkwarehouse") \
        .config("spark.sql.warehouse.dir", f"file:///{path_to_warehouse}") \
        .enableHiveSupport() \
        .getOrCreate()
    # Load JSON file into a DataFrame
    data = format_json(path_to_json)
    df = spark.createDataFrame(data)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    spark.sql(f"USE {database_name}")
    df.write.mode("overwrite").saveAsTable("aeroplane_models")
    print(spark.sql("SHOW TABLES").show())
    print(spark.sql("SELECT * FROM aeroplane_models").show())
    print(spark.sql("DESCRIBE TABLE aeroplane_models").show())

if __name__ == "__main__":
    main()
