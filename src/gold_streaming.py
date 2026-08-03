from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, sum as _sum, avg, window, date_format, current_timestamp,
)
from src.common.paths import (
    CATALOG, SCHEMA, CHECKPOINT_GOLD, TBL_SILVER_EVENTS, TBL_REJECTED,
    TBL_GOLD_SUMMARY, TBL_GOLD_HOURLY, TBL_GOLD_DAILY,
)

spark = SparkSession.builder.getOrCreate()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_GOLD_SUMMARY} (
  order_count_cumulative BIGINT, order_count_rolling_24h BIGINT,
  total_amount_cumulative DOUBLE, avg_order_value_cumulative DOUBLE,
  rejection_count_cumulative BIGINT, rejection_count_rolling_24h BIGINT,
  active_silver_rows BIGINT, superseded_silver_rows BIGINT,
  computed_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TBL_GOLD_HOURLY} (hour TIMESTAMP, order_count BIGINT) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TBL_GOLD_DAILY} (day DATE, order_count BIGINT) USING DELTA")

WATERMARK = "2 hours"


def run():
    events = (spark.readStream.table(TBL_SILVER_EVENTS)
              .withWatermark("order_date", WATERMARK))

    def upsert_hourly(batch_df, batch_id):
        batch_df.createOrReplaceTempView("hourly_batch")
        spark.sql(f"""
        MERGE INTO {TBL_GOLD_HOURLY} t
        USING (SELECT window.start AS hour, window_count AS order_count FROM hourly_batch) s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET t.order_count = s.order_count
        WHEN NOT MATCHED THEN INSERT (hour, order_count) VALUES (s.hour, s.order_count)
        """)
        # prune anything older than the rolling window — cheap periodic cleanup, not itself streaming state
        spark.sql(f"DELETE FROM {TBL_GOLD_HOURLY} WHERE hour < current_timestamp() - INTERVAL 24 HOURS")

    # outputMode("update") is required here: the default ("append") only emits a
    # window once the watermark advances PAST its end — which needs a LATER batch
    # to trigger, and this job runs one AvailableNow shot then stops, so append
    # mode would never emit anything (observed live: hourly/daily trend tables
    # stayed empty despite silver_version_events being populated correctly).
    # "update" emits a window's current state as soon as it changes; Spark's
    # aggregation state already holds the full cumulative count for that window
    # across all batches, so each emission is the complete count, not a delta —
    # safe to overwrite on MERGE, which upsert_hourly/upsert_daily already do.
    (events.withWatermark("order_date", WATERMARK)
     .groupBy(window("order_date", "1 hour"))
     .agg(count("*").alias("window_count"))
     .writeStream
     .outputMode("update")
     .option("checkpointLocation", f"{CHECKPOINT_GOLD}/hourly")
     .trigger(availableNow=True)
     .foreachBatch(upsert_hourly)
     .start().awaitTermination())

    def upsert_daily(batch_df, batch_id):
        batch_df.createOrReplaceTempView("daily_batch")
        spark.sql(f"""
        MERGE INTO {TBL_GOLD_DAILY} t
        USING (SELECT CAST(window.start AS DATE) AS day, window_count AS order_count FROM daily_batch) s
        ON t.day = s.day
        WHEN MATCHED THEN UPDATE SET t.order_count = s.order_count
        WHEN NOT MATCHED THEN INSERT (day, order_count) VALUES (s.day, s.order_count)
        """)

    (events.withWatermark("order_date", WATERMARK)
     .groupBy(window("order_date", "1 day"))
     .agg(count("*").alias("window_count"))
     .writeStream
     .outputMode("update")
     .option("checkpointLocation", f"{CHECKPOINT_GOLD}/daily")
     .trigger(availableNow=True)
     .foreachBatch(upsert_daily)
     .start().awaitTermination())

    # Cumulative + rolling scalars: plain batch reads over the same append-only
    # sources are correct here (no ranking, no window state) and simpler than
    # forcing a second streaming query for single-row running totals.
    summary = spark.sql(f"""
    SELECT
      (SELECT count(*) FROM {TBL_SILVER_EVENTS}) AS order_count_cumulative,
      (SELECT count(*) FROM {TBL_SILVER_EVENTS} WHERE event_ts >= current_timestamp() - INTERVAL 24 HOURS) AS order_count_rolling_24h,
      (SELECT sum(quantity * unit_price) FROM {TBL_SILVER_EVENTS}) AS total_amount_cumulative,
      (SELECT avg(quantity * unit_price) FROM {TBL_SILVER_EVENTS}) AS avg_order_value_cumulative,
      (SELECT count(*) FROM {TBL_REJECTED}) AS rejection_count_cumulative,
      (SELECT count(*) FROM {TBL_REJECTED} WHERE rejected_at >= current_timestamp() - INTERVAL 24 HOURS) AS rejection_count_rolling_24h,
      CAST(NULL AS BIGINT) AS active_silver_rows,
      CAST(NULL AS BIGINT) AS superseded_silver_rows,
      current_timestamp() AS computed_at
    """)
    summary.createOrReplaceTempView("streaming_summary")
    spark.sql(f"""
    MERGE INTO {TBL_GOLD_SUMMARY} t USING streaming_summary s ON true
    WHEN MATCHED THEN UPDATE SET
      order_count_cumulative = s.order_count_cumulative,
      order_count_rolling_24h = s.order_count_rolling_24h,
      total_amount_cumulative = s.total_amount_cumulative,
      avg_order_value_cumulative = s.avg_order_value_cumulative,
      rejection_count_cumulative = s.rejection_count_cumulative,
      rejection_count_rolling_24h = s.rejection_count_rolling_24h,
      computed_at = s.computed_at
    WHEN NOT MATCHED THEN INSERT (order_count_cumulative, order_count_rolling_24h, total_amount_cumulative,
      avg_order_value_cumulative, rejection_count_cumulative, rejection_count_rolling_24h, computed_at)
      VALUES (s.order_count_cumulative, s.order_count_rolling_24h, s.total_amount_cumulative,
      s.avg_order_value_cumulative, s.rejection_count_cumulative, s.rejection_count_rolling_24h, s.computed_at)
    """)


if __name__ == "__main__":
    run()
