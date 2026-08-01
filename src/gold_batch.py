from pyspark.sql import SparkSession
from src.common.paths import CATALOG, SCHEMA, TBL_SILVER, TBL_GOLD_TOP_ITEMS, TBL_GOLD_SUMMARY

spark = SparkSession.builder.getOrCreate()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_GOLD_TOP_ITEMS} (
  item_rank INT, item_name STRING, order_count BIGINT
) USING DELTA
""")


def run():
    top_items = spark.sql(f"""
    SELECT row_number() OVER (ORDER BY count(*) DESC) AS item_rank, item_name, count(*) AS order_count
    FROM {TBL_SILVER} WHERE is_current = true
    GROUP BY item_name ORDER BY order_count DESC LIMIT 5
    """)
    top_items.write.format("delta").mode("overwrite").saveAsTable(TBL_GOLD_TOP_ITEMS)

    snapshot = spark.sql(f"""
    SELECT
      (SELECT count(*) FROM {TBL_SILVER} WHERE is_current = true) AS active_rows,
      (SELECT count(*) FROM {TBL_SILVER} WHERE is_current = false) AS superseded_rows
    """)
    snapshot.createOrReplaceTempView("scd2_snapshot")
    spark.sql(f"""
    UPDATE {TBL_GOLD_SUMMARY} SET
      active_silver_rows = (SELECT active_rows FROM scd2_snapshot),
      superseded_silver_rows = (SELECT superseded_rows FROM scd2_snapshot)
    """)


if __name__ == "__main__":
    run()
