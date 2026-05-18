from spark_session import get_spark_session


def extract_from_csv(file_path):
    # Dodatni CSV izvor cita se sa headerom i automatskim prepoznavanjem tipova.
    spark = get_spark_session("ATP ETL - CSV Extract")
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )
