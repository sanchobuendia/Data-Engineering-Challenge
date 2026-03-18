from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, sha2, concat_ws, to_timestamp


def normalize_trips(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("trip_datetime", to_timestamp(col("trip_datetime")))
        .withColumn(
            "time_of_day",
            expr(
                "CASE "
                "WHEN hour(trip_datetime) BETWEEN 0 AND 5 THEN 'night' "
                "WHEN hour(trip_datetime) BETWEEN 6 AND 11 THEN 'morning' "
                "WHEN hour(trip_datetime) BETWEEN 12 AND 17 THEN 'afternoon' "
                "ELSE 'evening' END"
            ),
        )
        .withColumn("origin_cell", expr("concat(round(origin_lat, 3), ',', round(origin_lon, 3))"))
        .withColumn(
            "destination_cell",
            expr("concat(round(destination_lat, 3), ',', round(destination_lon, 3))"),
        )
        .withColumn(
            "event_hash",
            sha2(
                concat_ws(
                    "|",
                    col("city"),
                    col("region"),
                    col("origin_lat"),
                    col("origin_lon"),
                    col("destination_lat"),
                    col("destination_lon"),
                    col("trip_datetime"),
                    col("datasource"),
                ),
                256,
            ),
        )
    )
