from pyspark.sql.functions import col, initcap, lit, trim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_country_dim(country_df, csv_matches_df=None):
    # -------------------------------------------------------------------------
    # MYSQL IZVOR
    # Drzave iz relacijske baze vec imaju country_id, populaciju i regiju.
    # -------------------------------------------------------------------------
    mysql_countries = country_df.select(
        col("id").cast("int").alias("country_id"),
        initcap(trim(col("name"))).alias("name"),
        col("population").cast("int").alias("population"),
        initcap(trim(col("region"))).alias("region"),
    )

    if csv_matches_df is not None:
        # ---------------------------------------------------------------------
        # CSV IZVOR
        # Iz CSV-a uzimam drzave turnira, pobjednika i porazenog igraca jer se
        # drzava moze pojaviti u bilo kojem od ta tri stupca.
        # ---------------------------------------------------------------------
        csv_countries = (
            csv_matches_df.selectExpr("tourney_country as name")
            .unionByName(csv_matches_df.selectExpr("winner_country as name"))
            .unionByName(csv_matches_df.selectExpr("loser_country as name"))
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("country_id", lit(None).cast("int"))
            .withColumn("population", lit(None).cast("int"))
            .withColumn("region", lit(None).cast("string"))
        )
        combined_df = mysql_countries.unionByName(csv_countries)
    else:
        combined_df = mysql_countries

    # -------------------------------------------------------------------------
    # FINALNA DIMENZIJA
    # Dimenzija drzava zadrzava jedinstvene nazive drzava i dobiva country_tk.
    # -------------------------------------------------------------------------
    window = Window.orderBy("name")
    return (
        combined_df
        .dropna(subset=["name"])
        .dropDuplicates(["name"])
        .withColumn("country_tk", row_number().over(window))
        .select("country_tk", "country_id", "name", "population", "region")
    )
