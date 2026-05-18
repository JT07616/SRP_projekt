from pyspark.sql.functions import col, dayofmonth, month, quarter, to_date, year
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def transform_date_dim(tournament_df, csv_matches_df=None):
    # -------------------------------------------------------------------------
    # MYSQL IZVOR
    # Iz relacijske tablice tournament uzimam start_date kao datum turnira.
    # -------------------------------------------------------------------------
    mysql_dates = tournament_df.select(to_date(col("start_date")).alias("date"))

    if csv_matches_df is not None:
        # ---------------------------------------------------------------------
        # CSV IZVOR
        # CSV dodaje datume turnira iz processed_20 skupa.
        # ---------------------------------------------------------------------
        csv_dates = csv_matches_df.select(to_date(col("tourney_date")).alias("date"))
        combined_df = mysql_dates.unionByName(csv_dates)
    else:
        combined_df = mysql_dates

    # -------------------------------------------------------------------------
    # FINALNA DIMENZIJA
    # Jedan redak u dim_date predstavlja jedan jedinstveni datum, a datum se
    # dodatno rastavlja na dan, mjesec, kvartal i godinu.
    # -------------------------------------------------------------------------
    window = Window.orderBy("date")
    return (
        combined_df
        .dropna(subset=["date"])
        .dropDuplicates(["date"])
        .withColumn("date_tk", row_number().over(window))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("year", year(col("date")))
        .select("date_tk", "date", "day", "month", "quarter", "year")
    )
