from src.common.hashing import compute_record_hash, compute_version_id

def test_same_inputs_produce_same_hash():
    h1 = compute_record_hash("CUST-1", "Widget A", 2, 12.50, "2026-07-25")
    h2 = compute_record_hash("CUST-1", "Widget A", 2, 12.50, "2026-07-25")
    assert h1 == h2

def test_different_quantity_produces_different_hash():
    h1 = compute_record_hash("CUST-1", "Widget A", 2, 12.50, "2026-07-25")
    h2 = compute_record_hash("CUST-1", "Widget A", 3, 12.50, "2026-07-25")
    assert h1 != h2

def test_version_id_changes_when_hash_changes():
    v1 = compute_version_id("ORD-1", "hash-a")
    v2 = compute_version_id("ORD-1", "hash-b")
    assert v1 != v2

def test_version_id_stable_for_same_order_and_hash():
    v1 = compute_version_id("ORD-1", "hash-a")
    v2 = compute_version_id("ORD-1", "hash-a")
    assert v1 == v2
