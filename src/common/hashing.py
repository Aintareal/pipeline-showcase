import hashlib

def compute_record_hash(customer_id, item_name, quantity, unit_price, order_date) -> str:
    s = "||".join(str(x) for x in [customer_id, item_name, quantity, unit_price, order_date])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def compute_version_id(order_id, record_hash) -> str:
    s = f"{order_id}||{record_hash}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
