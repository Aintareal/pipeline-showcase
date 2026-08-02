import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, udf, current_timestamp, row_number, desc, to_json, struct,
)
from pyspark.sql.types import StringType
from src.common.hashing import compute_record_hash, compute_version_id
from src.common.validation import validate_order
from src.common.paths import (
    CATALOG, SCHEMA, CHECKPOINT_SILVER, TBL_BRONZE, TBL_SILVER,
    TBL_SILVER_EVENTS, TBL_REJECTED,
)

spark = SparkSession.builder.getOrCreate()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_SILVER} (
  order_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  order_id STRING, customer_id STRING, item_name STRING, quantity INT, unit_price DOUBLE,
  order_date TIMESTAMP, created_time TIMESTAMP, record_hash STRING, version_id STRING,
  effective_start_dt TIMESTAMP, effective_end_dt TIMESTAMP, is_current BOOLEAN
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_SILVER_EVENTS} (
  order_id STRING, customer_id STRING, item_name STRING, quantity INT, unit_price DOUBLE,
  order_date TIMESTAMP, created_time TIMESTAMP, record_hash STRING, version_id STRING, event_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_REJECTED} (
  order_id STRING, raw_payload STRING, rejection_reason STRING,
  _source_file STRING, _ingest_ts TIMESTAMP, rejected_at TIMESTAMP
) USING DELTA
""")

record_hash_udf = udf(compute_record_hash, StringType())
version_id_udf = udf(compute_version_id, StringType())


# validate_order(record: dict) expects a plain dict (record.get(...)) — a
# pyspark Row (what struct(...) produces at the UDF boundary) does not support
# .get() and raises PySparkAttributeError. Wrap it so the UDF is called with
# individual scalar columns and builds the dict itself, without touching
# validate_order's own signature/contract (src/common/validation.py, Task 2).
def _validate_order_fields(order_id, customer_id, item_name, quantity, unit_price, order_date):
    return validate_order({
        "order_id": order_id,
        "customer_id": customer_id,
        "item_name": item_name,
        "quantity": quantity,
        "unit_price": unit_price,
        "order_date": order_date,
    })


validate_udf = udf(_validate_order_fields, StringType())


def process_batch(microbatch_df, batch_id):
    microbatch_df.persist()

    validated = microbatch_df.withColumn(
        "rejection_reason",
        validate_udf(
            col("order_id"), col("customer_id"), col("item_name"),
            col("quantity"), col("unit_price"), col("order_date"),
        )
    )

    bad = validated.filter(col("rejection_reason").isNotNull())
    (bad.select(
        col("order_id"),
        to_json(struct(*[c for c in microbatch_df.columns])).alias("raw_payload"),
        col("rejection_reason"),
        col("_source_file"), col("_ingest_ts"),
        current_timestamp().alias("rejected_at"),
     ).write.format("delta").mode("append").saveAsTable(TBL_REJECTED))

    good = (validated.filter(col("rejection_reason").isNull())
            .withColumn("record_hash", record_hash_udf(
                col("customer_id"), col("item_name"), col("quantity"),
                col("unit_price"), col("order_date")))
            .withColumn("version_id", version_id_udf(col("order_id"), col("record_hash"))))

    # De-dup within this microbatch: keep the latest version per order_id
    w = Window.partitionBy("order_id").orderBy(desc("created_time"), desc("_ingest_ts"))
    batch_current = (good.withColumn("_rn", row_number().over(w))
                          .filter(col("_rn") == 1).drop("_rn"))

    batch_current.createOrReplaceTempView("batch_current")
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW staged_changes AS
    SELECT cur.version_id AS join_version_id, 'close' AS action,
           CAST(NULL AS STRING) AS order_id, CAST(NULL AS STRING) AS customer_id,
           CAST(NULL AS STRING) AS item_name, CAST(NULL AS INT) AS quantity,
           CAST(NULL AS DOUBLE) AS unit_price, CAST(NULL AS TIMESTAMP) AS order_date,
           CAST(NULL AS TIMESTAMP) AS created_time, CAST(NULL AS STRING) AS record_hash,
           current_timestamp() AS effective_start_dt, current_timestamp() AS effective_end_dt,
           CAST(NULL AS BOOLEAN) AS is_current
    FROM {TBL_SILVER} cur
    JOIN batch_current src
      ON cur.order_id = src.order_id AND cur.is_current = true AND cur.record_hash <> src.record_hash

    UNION ALL

    SELECT version_id AS join_version_id, 'insert' AS action,
           order_id, customer_id, item_name, quantity, unit_price, order_date, created_time, record_hash,
           current_timestamp() AS effective_start_dt, CAST(NULL AS TIMESTAMP) AS effective_end_dt,
           true AS is_current
    FROM batch_current src
    WHERE NOT EXISTS (
      SELECT 1 FROM {TBL_SILVER} cur
      WHERE cur.order_id = src.order_id AND cur.is_current = true AND cur.record_hash = src.record_hash
    )
    """)

    # Snapshot of every version_id already in silver_orders *before* the MERGE
    # below mutates it — used after the MERGE to detect the revert/collision
    # edge case (see comment at new_versions below). Must be captured now: once
    # the MERGE runs, legitimate new inserts also become present in TBL_SILVER,
    # so checking post-MERGE would wrongly exclude every real insert too.
    spark.sql(f"SELECT version_id FROM {TBL_SILVER}") \
        .createOrReplaceTempView("silver_version_ids_before_merge")

    spark.sql(f"""
    MERGE INTO {TBL_SILVER} AS tgt
    USING staged_changes AS src
    ON tgt.version_id = src.join_version_id
    WHEN MATCHED AND src.action = 'close' THEN
      UPDATE SET tgt.is_current = false, tgt.effective_end_dt = src.effective_end_dt
    WHEN NOT MATCHED THEN
      INSERT (order_id, customer_id, item_name, quantity, unit_price, order_date, created_time,
              record_hash, version_id, effective_start_dt, effective_end_dt, is_current)
      VALUES (src.order_id, src.customer_id, src.item_name, src.quantity, src.unit_price, src.order_date,
              src.created_time, src.record_hash, src.join_version_id, src.effective_start_dt,
              src.effective_end_dt, src.is_current)
    """)

    # Append-only event log powering gold's real streaming aggregation (Task 5) —
    # only rows that actually became a new current version (excludes true no-op
    # duplicates). Also excludes the documented revert/collision edge case: if a
    # reverted value's version_id collides with an older *superseded* row's
    # version_id, the MERGE's "insert" row matches an existing tgt row on
    # version_id but there is no `WHEN MATCHED AND action='insert'` clause, so it
    # silently no-ops and no row actually lands in silver_orders. We detect that
    # by checking against silver_version_ids_before_merge (captured above, prior
    # to the MERGE): a colliding version_id was already present in silver_orders
    # before this batch ran, whereas a genuinely new insert's version_id was not.
    # This keeps the event log from recording a phantom version that was never
    # actually written, which would otherwise inflate Task 5's streaming
    # aggregation counts.
    new_versions = spark.sql("""
        SELECT sc.* FROM staged_changes sc
        WHERE sc.action = 'insert'
          AND NOT EXISTS (
            SELECT 1 FROM silver_version_ids_before_merge v
            WHERE v.version_id = sc.join_version_id
          )
        """) \
        .drop("action", "effective_start_dt", "effective_end_dt", "is_current") \
        .withColumnRenamed("join_version_id", "version_id") \
        .withColumn("event_ts", current_timestamp())
    new_versions.write.format("delta").mode("append").saveAsTable(TBL_SILVER_EVENTS)

    microbatch_df.unpersist()


bronze_stream = spark.readStream.table(TBL_BRONZE)
(bronze_stream.writeStream
 .option("checkpointLocation", CHECKPOINT_SILVER)
 .trigger(availableNow=True)
 .foreachBatch(process_batch)
 .start()
 .awaitTermination())
