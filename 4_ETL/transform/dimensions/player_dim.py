from pyspark.sql.functions import col, initcap, lit, trim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_player_dim(player_df, country_df, csv_matches_df=None):
    # -------------------------------------------------------------------------
    # MYSQL IZVOR
    # Igraci iz relacijske baze spajaju se s tablicom country kako bi dimenzija
    # imala i naziv drzave te regiju igraca.
    # -------------------------------------------------------------------------
    mysql_players = (
        player_df.alias("p")
        .join(country_df.alias("c"), col("p.country_fk") == col("c.id"), "left")
        .select(
            col("p.id").cast("int").alias("player_id"),
            initcap(trim(col("p.name"))).alias("name"),
            trim(col("p.hand")).alias("hand"),
            col("p.height").cast("int").alias("height"),
            initcap(trim(col("c.name"))).alias("country_name"),
            initcap(trim(col("c.region"))).alias("region"),
        )
    )

    if csv_matches_df is not None:
        # ---------------------------------------------------------------------
        # CSV IZVOR
        # CSV ima odvojene stupce za winner i loser igrace. Te stupce pretvaram
        # u isti format kao MySQL podatke i zatim ih spajam u jednu dimenziju.
        # ---------------------------------------------------------------------
        winner_players = csv_matches_df.select(
            col("winner_id").cast("int").alias("player_id"),
            initcap(trim(col("winner_name"))).alias("name"),
            trim(col("winner_hand")).alias("hand"),
            col("winner_ht").cast("int").alias("height"),
            initcap(trim(col("winner_country"))).alias("country_name"),
        )
        loser_players = csv_matches_df.select(
            col("loser_id").cast("int").alias("player_id"),
            initcap(trim(col("loser_name"))).alias("name"),
            trim(col("loser_hand")).alias("hand"),
            col("loser_ht").cast("int").alias("height"),
            initcap(trim(col("loser_country"))).alias("country_name"),
        )
        csv_players = (
            winner_players
            .unionByName(loser_players)
            .withColumn("region", lit(None).cast("string"))
        )
        combined_df = mysql_players.unionByName(csv_players, allowMissingColumns=True)
    else:
        combined_df = mysql_players

    # -------------------------------------------------------------------------
    # FINALNA DIMENZIJA
    # Uklanjaju se duplikati po player_id i generira se player_tk kao
    # surogatni kljuc dimenzije igraca.
    # -------------------------------------------------------------------------
    window = Window.orderBy("player_id", "name")
    return (
        combined_df
        .dropna(subset=["player_id", "name"])
        .dropDuplicates(["player_id"])
        .withColumn("player_tk", row_number().over(window))
        .select("player_tk", "player_id", "name", "hand", "height", "country_name", "region")
    )
