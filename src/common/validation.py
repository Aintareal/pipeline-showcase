from datetime import datetime, timezone

REQUIRED_FIELDS = ["order_id", "customer_id", "item_name", "quantity", "unit_price", "order_date"]

def validate_order(record: dict) -> str | None:
    if any(record.get(f) in (None, "") for f in REQUIRED_FIELDS):
        return "missing_required_field"
    quantity = record["quantity"]
    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
        return "invalid_quantity"
    if quantity * record["unit_price"] <= 0:
        return "invalid_amount"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if record["order_date"] > now_iso:
        return "future_order_date"
    return None
