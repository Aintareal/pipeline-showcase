from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import window, count, row_number, desc

WATERMARK = "2 hours"


@dp.table(name="_gold_hourly_trend_raw", comment="Internal — unbounded hourly counts, see gold_hourly_trend.")
def _gold_hourly_trend_raw():
    events = dp.read_stream("silver_version_events").withWatermark("order_date", WATERMARK)
    return (events.groupBy(window("order_date", "1 hour")).agg(count("*").alias("order_count"))
            .selectExpr("window.start AS hour", "order_count"))


@dp.table(name="gold_hourly_trend")
def gold_hourly_trend():
    return dp.read("_gold_hourly_trend_raw").where("hour >= current_timestamp() - INTERVAL 24 HOURS")


@dp.table(name="gold_daily_trend")
def gold_daily_trend():
    events = dp.read_stream("silver_version_events").withWatermark("order_date", WATERMARK)
    return (events.groupBy(window("order_date", "1 day")).agg(count("*").alias("order_count"))
            .selectExpr("CAST(window.start AS DATE) AS day", "order_count"))


@dp.table(name="gold_dashboard_summary")
def gold_dashboard_summary():
    return spark.sql("""
      SELECT
        (SELECT count(*) FROM silver_version_events) AS order_count_cumulative,
        (SELECT count(*) FROM silver_version_events WHERE event_ts >= current_timestamp() - INTERVAL 24 HOURS) AS order_count_rolling_24h,
        (SELECT sum(quantity * unit_price) FROM silver_version_events) AS total_amount_cumulative,
        (SELECT avg(quantity * unit_price) FROM silver_version_events) AS avg_order_value_cumulative,
        (SELECT count(*) FROM rejected_orders) AS rejection_count_cumulative,
        (SELECT count(*) FROM rejected_orders WHERE rejected_at >= current_timestamp() - INTERVAL 24 HOURS) AS rejection_count_rolling_24h,
        (SELECT count(*) FROM silver_orders_current WHERE is_current = true) AS active_silver_rows,
        (SELECT count(*) FROM silver_orders_current WHERE is_current = false) AS superseded_silver_rows,
        current_timestamp() AS computed_at
    """)


@dp.table(name="gold_top_items")
def gold_top_items():
    current = dp.read("silver_orders_current").filter("is_current = true")
    counts = current.groupBy("item_name").count().withColumnRenamed("count", "order_count")
    ranked = counts.withColumn("item_rank", row_number().over(Window.orderBy(desc("order_count"))))
    return ranked.select("item_rank", "item_name", "order_count").orderBy(desc("order_count")).limit(5)
