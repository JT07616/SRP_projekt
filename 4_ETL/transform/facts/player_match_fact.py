from pyspark.sql.functions import col, row_number, to_date, trim, when
from pyspark.sql.window import Window


def _build_mysql_player_rows(raw_data):
    # -------------------------------------------------------------------------
    # MYSQL IZVOR -> FACT REDOVI
    # Relacijski model vec ima tablicu player_match_stats, gdje postoji jedan
    # zapis po igracu u mecu. Zato iz MySQL izvora direktno gradimo redove za
    # fact tablicu i spajamo ih s mecem, turnirom, igracem i drzavom.
    # -------------------------------------------------------------------------
    pms = raw_data["player_match_stats"].alias("pms")
    match_df = raw_data["match"].alias("m")
    tournament_df = raw_data["tournament"].alias("t")
    player_df = raw_data["player"].alias("p")
    country_df = raw_data["country"].alias("c")

    return (
        pms
        .join(match_df, col("pms.match_fk") == col("m.id"), "inner")
        .join(tournament_df, col("m.tournament_fk") == col("t.id"), "inner")
        .join(player_df, col("pms.player_fk") == col("p.id"), "inner")
        .join(country_df, col("p.country_fk") == col("c.id"), "left")
        .select(
            to_date(col("t.start_date")).alias("date"),
            col("pms.player_fk").cast("int").alias("player_id"),
            when(col("pms.player_fk") == col("m.winner_fk"), col("m.loser_fk"))
            .otherwise(col("m.winner_fk"))
            .cast("int")
            .alias("opponent_player_id"),
            col("t.id").cast("int").alias("tournament_id"),
            trim(col("c.name")).alias("country_name"),
            trim(col("m.score")).alias("score"),
            col("m.best_of").cast("int").alias("best_of"),
            trim(col("m.round")).alias("round"),
            trim(col("pms.seed")).alias("seed"),
            trim(col("pms.entry")).alias("entry"),
            col("pms.rank").cast("int").alias("rank"),
            col("m.match_num").cast("int").alias("match_num"),
            col("pms.ace").cast("int").alias("ace"),
            col("pms.double_fault").cast("int").alias("double_fault"),
            col("pms.service_points").cast("int").alias("service_points"),
            col("pms.first_in").cast("int").alias("first_in"),
            col("pms.first_won").cast("int").alias("first_won"),
            col("pms.second_won").cast("int").alias("second_won"),
            col("pms.service_games").cast("int").alias("service_games"),
            col("pms.break_points_saved").cast("int").alias("break_points_saved"),
            col("pms.break_points_faced").cast("int").alias("break_points_faced"),
            col("pms.rank_points").cast("int").alias("rank_points"),
            col("m.minutes").cast("int").alias("minutes"),
        )
    )


def _build_csv_player_rows(csv_matches_df):
    # -------------------------------------------------------------------------
    # CSV IZVOR -> FACT REDOVI
    # U CSV-u jedan redak predstavlja jedan mec. Buduci da moja fact tablica
    # prati statistiku po igracu u mecu, svaki CSV redak razdvajam na dva reda:
    # winner_rows za pobjednika i loser_rows za porazenog igraca.
    # -------------------------------------------------------------------------
    winner_rows = csv_matches_df.select(
        to_date(col("tourney_date")).alias("date"),
        col("winner_id").cast("int").alias("player_id"),
        col("loser_id").cast("int").alias("opponent_player_id"),
        col("tourney_id").cast("int").alias("tournament_id"),
        trim(col("winner_country")).alias("country_name"),
        trim(col("score")).alias("score"),
        col("best_of").cast("int").alias("best_of"),
        trim(col("round")).alias("round"),
        trim(col("winner_seed")).alias("seed"),
        trim(col("winner_entry")).alias("entry"),
        col("winner_rank").cast("int").alias("rank"),
        col("match_num").cast("int").alias("match_num"),
        col("w_ace").cast("int").alias("ace"),
        col("w_df").cast("int").alias("double_fault"),
        col("w_svpt").cast("int").alias("service_points"),
        col("w_1stIn").cast("int").alias("first_in"),
        col("w_1stWon").cast("int").alias("first_won"),
        col("w_2ndWon").cast("int").alias("second_won"),
        col("w_SvGms").cast("int").alias("service_games"),
        col("w_bpSaved").cast("int").alias("break_points_saved"),
        col("w_bpFaced").cast("int").alias("break_points_faced"),
        col("winner_rank_points").cast("int").alias("rank_points"),
        col("minutes").cast("int").alias("minutes"),
    )

    loser_rows = csv_matches_df.select(
        to_date(col("tourney_date")).alias("date"),
        col("loser_id").cast("int").alias("player_id"),
        col("winner_id").cast("int").alias("opponent_player_id"),
        col("tourney_id").cast("int").alias("tournament_id"),
        trim(col("loser_country")).alias("country_name"),
        trim(col("score")).alias("score"),
        col("best_of").cast("int").alias("best_of"),
        trim(col("round")).alias("round"),
        trim(col("loser_seed")).alias("seed"),
        trim(col("loser_entry")).alias("entry"),
        col("loser_rank").cast("int").alias("rank"),
        col("match_num").cast("int").alias("match_num"),
        col("l_ace").cast("int").alias("ace"),
        col("l_df").cast("int").alias("double_fault"),
        col("l_svpt").cast("int").alias("service_points"),
        col("l_1stIn").cast("int").alias("first_in"),
        col("l_1stWon").cast("int").alias("first_won"),
        col("l_2ndWon").cast("int").alias("second_won"),
        col("l_SvGms").cast("int").alias("service_games"),
        col("l_bpSaved").cast("int").alias("break_points_saved"),
        col("l_bpFaced").cast("int").alias("break_points_faced"),
        col("loser_rank_points").cast("int").alias("rank_points"),
        col("minutes").cast("int").alias("minutes"),
    )

    return winner_rows.unionByName(loser_rows)


def transform_player_match_fact(
    raw_data,
    dim_date_df,
    dim_player_df,
    dim_tournament_df,
    dim_country_df,
    dim_match_info_df,
):
    # -------------------------------------------------------------------------
    # SPAJANJE IZVORA
    # Ovdje spajam fact redove iz dva izvora:
    # 1) MySQL relacijska baza - podaci iz processed_80
    # 2) CSV datoteka - podaci iz processed_20
    # -------------------------------------------------------------------------
    combined_rows = _build_mysql_player_rows(raw_data)

    csv_matches_df = raw_data.get("csv_matches")
    if csv_matches_df is not None:
        combined_rows = combined_rows.unionByName(_build_csv_player_rows(csv_matches_df))

    # -------------------------------------------------------------------------
    # POVEZIVANJE S DIMENZIJAMA
    # Fact tablica ne sprema tekstualne opise dimenzija, nego surogatne kljuceve.
    # Zato se redovi spajaju s dim_date, dim_player, dim_tournament,
    # dim_country i dim_match_info kako bi se dohvatili *_tk kljucevi.
    # -------------------------------------------------------------------------
    fact_df = (
        combined_rows.alias("f")
        .join(dim_date_df.alias("dd"), col("f.date") == col("dd.date"), "left")
        .join(dim_player_df.alias("dp"), col("f.player_id") == col("dp.player_id"), "left")
        .join(
            dim_player_df.alias("dop"),
            col("f.opponent_player_id") == col("dop.player_id"),
            "left",
        )
        .join(
            dim_tournament_df.alias("dt"),
            col("f.tournament_id") == col("dt.tournament_id"),
            "left",
        )
        .join(dim_country_df.alias("dc"), col("f.country_name") == col("dc.name"), "left")
        .join(
            dim_match_info_df.alias("dmi"),
            col("f.score").eqNullSafe(col("dmi.score"))
            & col("f.best_of").eqNullSafe(col("dmi.best_of"))
            & col("f.round").eqNullSafe(col("dmi.round"))
            & col("f.seed").eqNullSafe(col("dmi.seed"))
            & col("f.entry").eqNullSafe(col("dmi.entry"))
            & col("f.rank").eqNullSafe(col("dmi.rank")),
            "left",
        )
        .select(
            col("dd.date_tk"),
            col("dp.player_tk"),
            col("dop.player_tk").alias("opponent_player_tk"),
            col("dt.tournament_tk"),
            col("dc.country_tk"),
            col("dmi.match_info_tk"),
            col("f.match_num"),
            col("f.ace"),
            col("f.double_fault"),
            col("f.service_points"),
            col("f.first_in"),
            col("f.first_won"),
            col("f.second_won"),
            col("f.service_games"),
            col("f.break_points_saved"),
            col("f.break_points_faced"),
            col("f.rank_points"),
            col("f.minutes"),
        )
    )

    # -------------------------------------------------------------------------
    # GENERIRANJE FACT KLJUCA I FINALNA PROVJERA
    # fact_player_match_tk je surogatni kljuc fact tablice.
    # Nakon ciscenja postoji 43159 meceva, a svaki mec daje dva fact zapisa,
    # pa je ocekivani broj redaka 86318.
    # -------------------------------------------------------------------------
    window = Window.orderBy("date_tk", "tournament_tk", "match_num", "player_tk")
    final_df = fact_df.withColumn("fact_player_match_tk", row_number().over(window)).select(
        "fact_player_match_tk",
        "date_tk",
        "player_tk",
        "opponent_player_tk",
        "tournament_tk",
        "country_tk",
        "match_info_tk",
        "match_num",
        "ace",
        "double_fault",
        "service_points",
        "first_in",
        "first_won",
        "second_won",
        "service_games",
        "break_points_saved",
        "break_points_faced",
        "rank_points",
        "minutes",
    )
    assert final_df.count() == 86318, "Number of player-match fact records from cleaned ATP dataset."

    return final_df
