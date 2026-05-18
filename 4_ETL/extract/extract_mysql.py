from spark_session import get_spark_session


JDBC_URL = "jdbc:mysql://127.0.0.1:3306/tenis?useSSL=false&allowPublicKeyRetrieval=true"
CONNECTION_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver",
}


def extract_table(table_name):
    # Citanje jedne relacijske tablice iz MySQL baze preko JDBC konekcije.
    spark = get_spark_session("ATP ETL - MySQL Extract")
    return spark.read.jdbc(
        url=JDBC_URL,
        table=table_name,
        properties=CONNECTION_PROPERTIES,
    )


def extract_all_tables():
    # Relacijske tablice koje su izvor za dimenzijski model.
    return {
        "country": extract_table("country"),
        "tournament": extract_table("tournament"),
        "player": extract_table("player"),
        "match": extract_table("`match`"),
        "player_match_stats": extract_table("player_match_stats"),
    }
