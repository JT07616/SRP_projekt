from pyspark.sql.functions import col, initcap, trim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_tournament_dim(tournament_df, country_df, csv_matches_df=None):
    # -------------------------------------------------------------------------
    # MYSQL IZVOR
    # Turniri iz relacijske baze spajaju se s drzavom kako bi se dobili
    # country_name i region kao dio lokacijske hijerarhije.
    # -------------------------------------------------------------------------
    mysql_tournaments = (
        tournament_df.alias("t")
        .join(country_df.alias("c"), col("t.country_fk") == col("c.id"), "left")
        .select(
            col("t.id").cast("int").alias("tournament_id"),
            initcap(trim(col("t.name"))).alias("name"),
            initcap(trim(col("t.surface"))).alias("surface"),
            col("t.draw_size").cast("int").alias("draw_size"),
            trim(col("t.level")).alias("level"),
            initcap(trim(col("t.city"))).alias("city"),
            initcap(trim(col("c.name"))).alias("country_name"),
            initcap(trim(col("c.region"))).alias("region"),
        )
    )

    if csv_matches_df is not None:
        # ---------------------------------------------------------------------
        # CSV IZVOR
        # CSV donosi turnire iz processed_20 skupa, koji nisu bili ucitani
        # u relacijsku bazu kroz processed_80.
        # ---------------------------------------------------------------------
        csv_tournaments = csv_matches_df.select(
            col("tourney_id").cast("int").alias("tournament_id"),
            initcap(trim(col("tourney_name"))).alias("name"),
            initcap(trim(col("surface"))).alias("surface"),
            col("draw_size").cast("int").alias("draw_size"),
            trim(col("tourney_level")).alias("level"),
            initcap(trim(col("tourney_city"))).alias("city"),
            initcap(trim(col("tourney_country"))).alias("country_name"),
        )
        combined_df = mysql_tournaments.unionByName(csv_tournaments, allowMissingColumns=True)
    else:
        combined_df = mysql_tournaments

    # -------------------------------------------------------------------------
    # FINALNA DIMENZIJA
    # Jedan redak u dim_tournament predstavlja jedan jedinstveni turnir.
    # tournament_tk je surogatni kljuc.
    # -------------------------------------------------------------------------
    window = Window.orderBy("tournament_id", "name")
    return (
        combined_df
        .dropna(subset=["tournament_id", "name"])
        .dropDuplicates(["tournament_id"])
        .withColumn("tournament_tk", row_number().over(window))
        .select(
            "tournament_tk",
            "tournament_id",
            "name",
            "surface",
            "draw_size",
            "level",
            "city",
            "country_name",
            "region",
        )
    )
