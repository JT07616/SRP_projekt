import os
from pyspark.sql import SparkSession


def get_spark_session(app_name="ATP_ETL_App"):
    # MySQL JDBC driver se koristi da Spark moze citati i pisati u MySQL bazu.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.dirname(os.path.dirname(base_dir))
    mysql_connector_path = os.path.join(
        workspace_dir,
        "FIPU_srp_vjezbe",
        "Vjezbe4_ETL",
        "Connectors",
        "mysql-connector-j-9.2.0.jar",
    )

    # SparkSession - ulazna tocka za rad s PySpark DataFrameovima.
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", mysql_connector_path)
        .getOrCreate()
    )
