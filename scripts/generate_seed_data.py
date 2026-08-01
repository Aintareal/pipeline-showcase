import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT_DIR = Path(__file__).parent / "seed_files"
OUT_DIR.mkdir(exist_ok=True)

SEED_ORDERS = [
    {"order_id": "ORD-10001", "customer_id": "CUST-1001", "item_name": "Widget A", "quantity": 2, "unit_price": 12.50, "hours_ago": 24 * 3 + 3},
    {"order_id": "ORD-10002", "customer_id": "CUST-1002", "item_name": "Camp Stove", "quantity": 1, "unit_price": 54.00, "hours_ago": 24 * 2 + 5},
    {"order_id": "ORD-10003", "customer_id": "CUST-1003", "item_name": "Trail Runner Backpack", "quantity": 1, "unit_price": 89.99, "hours_ago": 24 * 2 + 1},
    {"order_id": "ORD-10004", "customer_id": "CUST-1004", "item_name": "Widget A", "quantity": 3, "unit_price": 12.50, "hours_ago": 24 + 6},
    {"order_id": "ORD-10005", "customer_id": "CUST-1005", "item_name": "Camp Stove", "quantity": 2, "unit_price": 54.00, "hours_ago": 1},
]

for order in SEED_ORDERS:
    order_dt = datetime.now(timezone.utc) - timedelta(hours=order.pop("hours_ago"))
    record = {
        **order,
        "order_date": order_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    out_path = OUT_DIR / f"seed-{record['order_id']}.json"
    out_path.write_text(json.dumps(record, separators=(",", ":")))  # minified — one line
    print(f"wrote {out_path}")
