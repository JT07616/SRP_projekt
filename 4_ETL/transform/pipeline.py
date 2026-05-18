from transform.dimensions.country_dim import transform_country_dim
from transform.dimensions.date_dim import transform_date_dim
from transform.dimensions.match_info_dim import transform_match_info_dim
from transform.dimensions.player_dim import transform_player_dim
from transform.dimensions.tournament_dim import transform_tournament_dim
from transform.facts.player_match_fact import transform_player_match_fact


def run_transformations(raw_data):
    # -------------------------------------------------------------------------
    # 1. KREIRANJE DIMENZIJA
    # Dimenzijske tablice se kreiraju prve jer fact tablica kasnije koristi
    # njihove surogatne kljuceve: player_tk, tournament_tk, country_tk,
    # date_tk i match_info_tk.
    # -------------------------------------------------------------------------
    player_dim = transform_player_dim(
        raw_data["player"],
        raw_data["country"],
        csv_matches_df=raw_data.get("csv_matches"),
    )
    print("1️⃣ Player dimension complete")

    tournament_dim = transform_tournament_dim(
        raw_data["tournament"],
        raw_data["country"],
        csv_matches_df=raw_data.get("csv_matches"),
    )
    print("2️⃣ Tournament dimension complete")

    country_dim = transform_country_dim(
        raw_data["country"],
        csv_matches_df=raw_data.get("csv_matches"),
    )
    print("3️⃣ Country dimension complete")

    date_dim = transform_date_dim(
        raw_data["tournament"],
        csv_matches_df=raw_data.get("csv_matches"),
    )
    print("4️⃣ Date dimension complete")

    match_info_dim = transform_match_info_dim(
        raw_data["match"],
        raw_data["player_match_stats"],
        csv_matches_df=raw_data.get("csv_matches"),
    )
    print("5️⃣ Match info dimension complete")

    # -------------------------------------------------------------------------
    # 2. KREIRANJE FACT TABLICE
    # Fact tablica povezuje sve dimenzije i sadrzi mjerljive statistike igraca
    # u mecu, npr. ace, double_fault, rank_points i minutes.
    # -------------------------------------------------------------------------
    fact_player_match = transform_player_match_fact(
        raw_data,
        date_dim,
        player_dim,
        tournament_dim,
        country_dim,
        match_info_dim,
    )
    print("6️⃣ Player match fact complete")

    # -------------------------------------------------------------------------
    # 3. POVRAT TABLICA SPREMNIH ZA LOAD
    # Svaki kljuc u dictionaryju je naziv tablice u MySQL-u, a vrijednost je
    # Spark DataFrame koji ce se zapisati u bazu.
    # -------------------------------------------------------------------------
    return {
        "dim_player": player_dim,
        "dim_tournament": tournament_dim,
        "dim_country": country_dim,
        "dim_date": date_dim,
        "dim_match_info": match_info_dim,
        "fact_player_match": fact_player_match,
    }
