from pyspark import pipelines as dp
from pyspark.sql.functions import udf, col, current_timestamp, to_json, struct
from pyspark.sql.types import StringType

# NOTE: deliberately NOT importing validate_order from src.common.validation here.
# Confirmed live: the driver can import src.* (Phase 0 finding 8), but a Python UDF's
# closure is pickled and re-deserialized on WORKER processes, which raised
# `ModuleNotFoundError: No module named 'src'` — DLT's library-file mechanism doesn't
# distribute the surrounding src/ package tree to workers the way spark_python_task's
# full bundle-sync did in V1. src/common/validation.py is kept as the tested, canonical
# reference (still exercised by tests/test_validation.py) — this is a duplicated copy
# of the same logic, self-contained in this file so the UDF has no cross-module
# dependency to resolve on a worker. Keep both in sync if the rule set ever changes.
from datetime import datetime, timezone

REQUIRED_FIELDS = ["order_id", "customer_id", "item_name", "quantity", "unit_price", "order_date"]


def _validate_order_fields(order_id, customer_id, item_name, quantity, unit_price, order_date):
    record = {
        "order_id": order_id, "customer_id": customer_id, "item_name": item_name,
        "quantity": quantity, "unit_price": unit_price, "order_date": order_date,
    }
    if any(record.get(f) in (None, "") for f in REQUIRED_FIELDS):
        return "missing_required_field"
    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
        return "invalid_quantity"
    if quantity * unit_price <= 0:
        return "invalid_amount"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if order_date > now_iso:
        return "future_order_date"
    return None


validate_udf = udf(_validate_order_fields, StringType())

# Best-effort SQL mirrors of validate_order, for @dp.expect metrics ONLY — NULL
# predicates evaluate as "passing" (CHECK-constraint convention), a cosmetic
# metrics quirk that does NOT affect rejected_orders routing below.
EXPECTATIONS = {
    "valid_required_fields": (
        "order_id IS NOT NULL AND order_id <> '' AND customer_id IS NOT NULL AND customer_id <> '' "
        "AND item_name IS NOT NULL AND item_name <> '' AND quantity IS NOT NULL "
        "AND unit_price IS NOT NULL AND order_date IS NOT NULL"
    ),
    "valid_quantity": "quantity > 0",
    "valid_amount": "quantity * unit_price > 0",
    "valid_order_date": "order_date <= current_timestamp()",
}


@dp.view()
@dp.expect_all(EXPECTATIONS)
def validated_orders():
    bronze = dp.read_stream("bronze_orders")
    return bronze.withColumn(
        "rejection_reason",
        validate_udf(col("order_id"), col("customer_id"), col("item_name"),
                     col("quantity"), col("unit_price"), col("order_date")),
    )


@dp.table(name="rejected_orders", comment="Orders failing validate_order() — routing preserved from V1.")
def rejected_orders():
    v = dp.read_stream("validated_orders")
    bronze_cols = [c for c in v.columns if c != "rejection_reason"]
    return (v.filter(col("rejection_reason").isNotNull())
             .select(col("order_id"), to_json(struct(*bronze_cols)).alias("raw_payload"),
                      col("rejection_reason"), col("_source_file"), col("_ingest_ts"),
                      current_timestamp().alias("rejected_at")))


@dp.table(name="silver_orders_staged")
def silver_orders_staged():
    v = dp.read_stream("validated_orders")
    # order_date arrives as STRING (Auto Loader's inferColumnTypes doesn't auto-detect
    # ISO-8601 date-strings as TIMESTAMP) — validate_order's future-date check already
    # ran on the raw string above (unchanged logic, correct), so it's safe to cast here,
    # after validation, for the rows that actually proceed to AUTO CDC. Gold's watermark
    # requires a genuine TIMESTAMP column; a string that merely looks like one fails
    # with EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE.
    return (v.filter(col("rejection_reason").isNull())
             .drop("rejection_reason")
             .withColumn("order_date", col("order_date").cast("timestamp")))


# --- AUTO CDC: replaces the hand-built merge-key MERGE from silver_scd2.py, AND
# replaces compute_record_hash/compute_version_id (V1 only, see hashing.py) — AUTO
# CDC's own column-by-column comparison against the current row is the change
# detection mechanism now, no synthetic hash needed. ---
dp.create_streaming_table(
    name="silver_orders",
    comment="Current + historical order versions (SCD2), AUTO CDC-managed.",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
dp.create_auto_cdc_flow(
    target="silver_orders",
    source="silver_orders_staged",
    keys=["order_id"],
    sequence_by=struct(col("created_time")),
    stored_as_scd_type="2",
    track_history_except_column_list=["created_time"],
)
# Confirmed live: since created_time is both the excluded column AND the sequence_by
# column, this exclusion does NOT keep a created_time-only resubmission in-place —
# it still gets its own __START_AT/version row here. The real "exact duplicate is a
# no-op" guarantee is enforced downstream in silver_version_events (dedup on the
# tracked-value tuple), not by this exclusion. Left in place since it's harmless and
# still correctly named for intent; see silver_version_events for the actual fix.


# No is_current column is auto-generated by AUTO CDC (confirmed) — current-row
# identification is via __END_AT IS NULL.
@dp.view()
def silver_orders_current():
    return dp.read("silver_orders").withColumn("is_current", col("__END_AT").isNull())


WATERMARK = "2 hours"


TRACKED_COLS = ["customer_id", "item_name", "quantity", "unit_price", "order_date"]

# Append-only feed of genuinely new-or-changed versions, feeding gold's watermarked
# streaming aggregation. A plain _change_type = 'insert' filter is NOT sufficient on
# its own: confirmed live that a resubmission changing ONLY created_time (identical
# order_id/customer_id/item_name/quantity/unit_price/order_date) still gets its own
# __START_AT in silver_orders — track_history_except_column_list does not keep it
# in-place when the excluded column is also the sequence_by column.
#
# First attempt was dropDuplicatesWithinWatermark on the tracked-value tuple — wrong:
# it drops ANY repeat of a value combo seen before in the window, not just a repeat
# of the IMMEDIATELY PRECEDING version. That silently broke the documented AUTO CDC
# improvement over V1 (a genuine revert, e.g. quantity 2 -> 5 -> 2, must still count
# as a real event on the second "2", not be treated as a dup of the first).
#
# Correct fix: compare each version only to its immediate predecessor. Structured
# Streaming doesn't support row-based window functions (LAG) on a streaming source,
# so this is done as a stream-static join against silver_orders itself, linking each
# event's __START_AT to the prior row's __END_AT (AUTO CDC's own version-chain link)
# — a stream-static join is a supported Structured Streaming pattern, unlike
# stream-stream joins with arbitrary keys.
@dp.table(name="silver_version_events")
def silver_version_events():
    events = (spark.readStream.format("delta").option("readChangeFeed", "true").table("silver_orders")
              .filter("_change_type = 'insert'")
              .withWatermark("order_date", WATERMARK))
    prior = dp.read("silver_orders").select(
        *[col(c).alias(f"_prior_{c}") for c in ["order_id", "__END_AT"] + TRACKED_COLS]
    )
    joined = events.join(
        prior,
        (events["order_id"] == prior["_prior_order_id"]) & (events["__START_AT"] == prior["_prior___END_AT"]),
        "left",
    )
    changed_expr = col("_prior_order_id").isNull()
    for c in TRACKED_COLS:
        changed_expr = changed_expr | (col(c) != col(f"_prior_{c}"))
    return (joined.filter(changed_expr)
             .select("order_id", "customer_id", "item_name", "quantity", "unit_price", "order_date", "created_time")
             .withColumn("event_ts", current_timestamp()))
