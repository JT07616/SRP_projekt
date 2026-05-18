import os
import sys

from extract.extract_csv import extract_from_csv
from extract.extract_mysql import extract_all_tables
from load.run_loading import write_spark_df_to_mysql
from spark_session import get_spark_session
from transform.pipeline import run_transformations


os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
os.environ.pop("SPARK_HOME", None)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def main():
    # -------------------------------------------------------------------------
    # PRIPREMA CSV PUTANJE
    # CSV predstavlja drugi izvor podataka u ETL procesu, odnosno processed_20.
    # Putanja se slaze relativno prema projektu da skripta radi i kada se
    # pokrene iz foldera 4_ETL.
    # -------------------------------------------------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(base_dir)
    csv_path = os.path.join(
        project_dir,
        "2_relational_model",
        "processed",
        "atp_matches_processed_20.csv",
    )

    # -------------------------------------------------------------------------
    # SPARK SESIJA
    # SparkSession je potreban za citanje, transformaciju i zapis DataFrameova.
    # -------------------------------------------------------------------------
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # -------------------------------------------------------------------------
    # EXTRACT
    # Cita se prvi izvor: MySQL relacijska baza tenis.
    # Cita se drugi izvor: CSV datoteka atp_matches_processed_20.csv.
    # -------------------------------------------------------------------------
    print("🚀 Starting data extraction")
    mysql_data = extract_all_tables()
    csv_data = {"csv_matches": extract_from_csv(csv_path)}
    raw_data = {**mysql_data, **csv_data}
    print("✅ Data extraction completed")

    # -------------------------------------------------------------------------
    # TRANSFORM
    # Podaci iz oba izvora se ciste, spajaju i pripremaju za dimenzijski model.
    # -------------------------------------------------------------------------
    print("🚀 Starting data transformation")
    load_ready_tables = run_transformations(raw_data)
    print("✅ Data transformation completed")

    # -------------------------------------------------------------------------
    # LOAD
    # Transformirani DataFrameovi zapisuju se u MySQL bazu kao dimenzijske
    # i fact tablice.
    # -------------------------------------------------------------------------
    print("🚀 Starting data loading")
    for table_name, df in load_ready_tables.items():
        write_spark_df_to_mysql(df, table_name)
    print("👏 Data loading completed")


if __name__ == "__main__":
    main()
