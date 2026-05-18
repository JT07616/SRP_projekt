from pyspark.sql.functions import col, trim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_match_info_dim(match_df, player_match_stats_df, csv_matches_df=None):
    # -------------------------------------------------------------------------
    # MYSQL IZVOR
    # Match info dimenzija opisuje kontekst meca i igracevog statusa u mecu.
    # Iz MySQL-a se dobiva spajanjem match i player_match_stats tablica.
    # -------------------------------------------------------------------------
    mysql_match_info = (
        player_match_stats_df.alias("pms")
        .join(match_df.alias("m"), col("pms.match_fk") == col("m.id"), "inner")
        .select(
            trim(col("m.score")).alias("score"),
            col("m.best_of").cast("int").alias("best_of"),
            trim(col("m.round")).alias("round"),
            trim(col("pms.seed")).alias("seed"),
            trim(col("pms.entry")).alias("entry"),
            col("pms.rank").cast("int").alias("rank"),
        )
    )

    if csv_matches_df is not None:
        # ---------------------------------------------------------------------
        # CSV IZVOR
        # Za CSV se zasebno uzimaju winner i loser atributi jer pobjednik i
        # porazeni imaju razlicite seed, entry i rank vrijednosti.
        # ---------------------------------------------------------------------
        winner_info = csv_matches_df.select(
            trim(col("score")).alias("score"),
            col("best_of").cast("int").alias("best_of"),
            trim(col("round")).alias("round"),
            trim(col("winner_seed")).alias("seed"),
            trim(col("winner_entry")).alias("entry"),
            col("winner_rank").cast("int").alias("rank"),
        )
        loser_info = csv_matches_df.select(
            trim(col("score")).alias("score"),
            col("best_of").cast("int").alias("best_of"),
            trim(col("round")).alias("round"),
            trim(col("loser_seed")).alias("seed"),
            trim(col("loser_entry")).alias("entry"),
            col("loser_rank").cast("int").alias("rank"),
        )
        combined_df = mysql_match_info.unionByName(winner_info).unionByName(loser_info)
    else:
        combined_df = mysql_match_info

    # -------------------------------------------------------------------------
    # FINALNA DIMENZIJA
    # Uklanjaju se duplikati istih kombinacija opisa meca i generira se
    # match_info_tk kao surogatni kljuc.
    # -------------------------------------------------------------------------
    window = Window.orderBy("round", "best_of", "score", "seed", "entry", "rank")
    return (
        combined_df
        .dropDuplicates(["score", "best_of", "round", "seed", "entry", "rank"])
        .withColumn("match_info_tk", row_number().over(window))
        .select("match_info_tk", "score", "best_of", "round", "seed", "entry", "rank")
    )
