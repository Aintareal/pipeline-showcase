from src.common.validation import validate_order

VALID = {
    "order_id": "ORD-1", "customer_id": "CUST-1", "item_name": "Widget A",
    "quantity": 2, "unit_price": 12.50, "order_date": "2026-07-25",
}

def test_valid_order_passes():
    assert validate_order(VALID) is None

def test_missing_required_field():
    rec = {**VALID, "customer_id": None}
    assert validate_order(rec) == "missing_required_field"

def test_non_positive_quantity():
    rec = {**VALID, "quantity": 0}
    assert validate_order(rec) == "invalid_quantity"

def test_non_integer_quantity():
    rec = {**VALID, "quantity": 2.5}
    assert validate_order(rec) == "invalid_quantity"

def test_negative_unit_price_is_invalid_amount_not_invalid_quantity():
    rec = {**VALID, "unit_price": -5.00}
    assert validate_order(rec) == "invalid_amount"

def test_future_order_date():
    rec = {**VALID, "order_date": "2099-01-01"}
    assert validate_order(rec) == "future_order_date"
