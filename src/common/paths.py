CATALOG = "portfolio_demo"
SCHEMA = "orders"

INBOUND_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/inbound"
INTERNAL_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/pipeline_internal"

CHECKPOINT_BRONZE = f"{INTERNAL_VOLUME}/_checkpoints/bronze"
CHECKPOINT_SILVER = f"{INTERNAL_VOLUME}/_checkpoints/silver"
CHECKPOINT_GOLD = f"{INTERNAL_VOLUME}/_checkpoints/gold"
SCHEMA_LOCATION_BRONZE = f"{INTERNAL_VOLUME}/_schema/bronze"

TBL_BRONZE = f"{CATALOG}.{SCHEMA}.bronze_orders"
TBL_SILVER = f"{CATALOG}.{SCHEMA}.silver_orders"
TBL_SILVER_EVENTS = f"{CATALOG}.{SCHEMA}.silver_version_events"
TBL_REJECTED = f"{CATALOG}.{SCHEMA}.rejected_orders"
TBL_GOLD_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_dashboard_summary"
TBL_GOLD_TOP_ITEMS = f"{CATALOG}.{SCHEMA}.gold_top_items"
TBL_GOLD_HOURLY = f"{CATALOG}.{SCHEMA}.gold_hourly_trend"
TBL_GOLD_DAILY = f"{CATALOG}.{SCHEMA}.gold_daily_trend"
