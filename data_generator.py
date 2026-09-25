"""
StepRight data generator — batch-based, fully deterministic under a fixed seed.
 
Generates one named batch per run:
  - batch_0        : initial full snapshot (all 7 tables, full locked scale)
  - batch_1, 2, ... : incremental batches — new orders/customers/clickstream/inventory,
                       a handful of status/profile update events on existing rows, and
                       a small number of new product launches + price/discontinuation
                       updates on existing products. categories is intentionally left
                       unchanged in incremental batches — real category churn is rare
                       for a business like this, so we don't fake it just to exercise
                       a code path. The loader (setup/02_data_loader.py) handles a
                       source having nothing staged for a given batch as a normal,
                       expected case, not an error.
 
Referential integrity is enforced within a batch AND across batches, using a small
local state file (generator_state.json) that tracks IDs already generated, so an
incremental batch's new rows can safely reference earlier batches' rows.
 
DETERMINISM — two things had to be fixed for --seed to actually produce identical
output across runs, machines, and people, not just identical *choices*:
 
1. IDs. uuid.uuid4() draws from os.urandom() (real OS entropy), which completely
   ignores random.seed(). Every ID used to be different on every single run, even
   with a fixed seed. Fixed by seeded_uuid() below, which builds a valid UUID4
   from the seeded `random` module instead.
 
2. Dates. Several calls used real wall-clock time — datetime.now(), and Faker's
   end_date="now" — which meant the same --seed produced different absolute dates
   depending on what day you happened to run the script. Fixed with --as-of: every
   date-producing call takes an explicit reference_now parameter. Dev/UAT batches
   omit --as-of and get real current time (data that looks fresh). Integration-test
   batches pass a fixed --as-of, so --seed + --as-of together produce byte-identical
   output forever, regardless of who runs it or when. See GENERATOR_GUIDE.md.
 
Output is local only — this script does not upload anywhere. Once a batch is
generated, manually upload its folder into the dev.stepright.staging volume via the
Databricks workspace UI, then run setup/02_data_loader.py to load it into landing.
 
Usage:
  pip install faker
 
  # Dev/UAT — realistic, fresh, NOT deterministic (no --as-of):
  python data_generator.py --batch 0 --output-dir ./raw_sources
  python data_generator.py --batch 1 --output-dir ./raw_sources
 
  # Integration tests — fully deterministic, small, fast:
  python data_generator.py --batch 0 --seed 42 --as-of 2026-01-01 \\
      --output-dir ./it_batches --state-file ./it_batches/generator_state.json \\
      --n-customers 20 --n-orders 50 --n-products 15 --n-clickstream 200 --n-inventory-days 2
 
See GENERATOR_GUIDE.md for the full determinism guarantee and recommended values.
 
This script is standalone — it does not import anything from the Databricks pipeline
code, and does not require a Databricks runtime. It simulates StepRight's external
source systems, not our pipeline.
"""
 
import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
 
from faker import Faker
 
fake = Faker()
 
CATEGORY_DEFS = [
    ("CAT001", "Running", None),
    ("CAT002", "Casual", None),
    ("CAT003", "Formal", None),
    ("CAT004", "Sports", None),
    ("CAT005", "Boots", None),
    ("CAT006", "Sandals", None),
]
 
BRANDS = ["Nike", "Adidas", "Puma", "New Balance", "Skechers", "Clarks", "Timberland"]
COLOURS = ["Black", "White", "Navy", "Red", "Grey", "Brown", "Tan", "Olive"]
MATERIALS = ["Leather", "Suede", "Mesh", "Canvas", "Synthetic"]
GENDER_TARGETS = ["M", "F", "unisex", "kids"]
WAREHOUSES = ["WH-EAST", "WH-WEST", "WH-CENTRAL"]
PAYMENT_METHODS = ["credit_card", "paypal", "buy_now_pay_later", "gift_card"]
ORDER_STATUS_FLOW = ["pending", "confirmed", "shipped", "delivered"]
ORDER_STATUS_TERMINAL_BRANCHES = ["cancelled", "returned"]
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
RETURN_REASONS = ["wrong_size", "defective", "not_as_described", "changed_mind"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase", "search"]
REFERRERS = ["google", "instagram", "email", "direct", "affiliate"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
 
SUBFOLDERS = {
    "customers": "customers_cdc",
    "orders": "orders_cdc",
    "order_items": "order_items_cdc",
    "products": "products",
    "categories": "categories",
    "clickstream": "clickstream",
    "inventory": "inventory",
}
 
 
# ---------------------------------------------------------------------------
# Determinism helpers
# ---------------------------------------------------------------------------
 
def seeded_uuid() -> str:
    """
    A valid UUID4-format string, drawn from the seeded `random` module instead
    of uuid.uuid4()'s os.urandom(). This is the actual fix for reproducibility —
    uuid.uuid4() ignores random.seed() completely, so every ID used to differ
    on every run even with a fixed seed. This doesn't change the shape of IDs
    anywhere downstream, only where their randomness comes from.
    """
    return str(uuid.UUID(int=random.getrandbits(128), version=4))
 
 
def seeded_short_id() -> str:
    """8-char hex suffix for filenames, from the same seeded source."""
    return seeded_uuid().replace("-", "")[:8]
 
 
# ---------------------------------------------------------------------------
# State management (cross-batch referential integrity)
# ---------------------------------------------------------------------------
 
def load_state(path):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {
        "customer_ids": [],
        "product_ids": [],
        "product_skus": {},       # product_id -> sku
        "category_ids": [],
        "order_ids": [],
        "order_customer": {},     # order_id -> customer_id
        "order_status": {},       # order_id -> current status
        "next_batch": 0,
    }
 
 
def save_state(path, state):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def ts_ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
 
 
def write_json_lines(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
 
 
def write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
 
 
def cdc_event(op, after, event_ts, before=None):
    """event_ts is required, on purpose — there is no 'now' fallback anymore.
    Every call site must decide its own timestamp, so forgetting to pass one
    is a loud error, not a silent wall-clock dependency."""
    return {
        "op": op,
        "ts_ms": ts_ms(event_ts),
        "before": before,
        "after": after,
    }
 
 
# ---------------------------------------------------------------------------
# Batch 0 — initial full snapshot
# ---------------------------------------------------------------------------
 
def generate_categories(out_dir, batch_dir):
    rows = [{"category_id": c[0], "category_name": c[1], "parent_category_id": c[2] or "", "is_active": True} for c in CATEGORY_DEFS]
    write_csv(os.path.join(batch_dir, SUBFOLDERS["categories"], "categories.csv"),
              ["category_id", "category_name", "parent_category_id", "is_active"], rows)
    return [c[0] for c in CATEGORY_DEFS]
 
 
def generate_products(batch_dir, category_ids, reference_now, n=500):
    rows = []
    product_ids, product_skus = [], {}
    launch_start = reference_now - timedelta(days=2 * 365)
    launch_end = reference_now - timedelta(days=30)
    for _ in range(n):
        pid = seeded_uuid()
        brand = random.choice(BRANDS)
        size = round(random.uniform(3.0, 14.0) * 2) / 2
        colour = random.choice(COLOURS)
        sku = f"{brand[:2].upper()}-{fake.lexify('????').upper()}-{size}-{colour[:3].upper()}"
        cost = round(random.uniform(15, 90), 2)
        retail = round(cost * random.uniform(1.4, 2.2), 2)
        row = {
            "product_id": pid, "sku": sku,
            "product_name": f"{brand} {fake.word().capitalize()}",
            "brand": brand, "category_id": random.choice(category_ids),
            "gender_target": random.choice(GENDER_TARGETS), "size_uk": size,
            "colour": colour, "material": random.choice(MATERIALS),
            "cost_price": cost, "retail_price": retail,
            "is_active": True, "launch_date": fake.date_between(start_date=launch_start, end_date=launch_end).isoformat(),
        }
        rows.append(row)
        product_ids.append(pid)
        product_skus[pid] = sku
    write_csv(os.path.join(batch_dir, SUBFOLDERS["products"], "products.csv"), list(rows[0].keys()), rows)
    products_by_id = {r["product_id"]: r for r in rows}
    return product_ids, product_skus, products_by_id
 
 
def generate_customers(batch_dir, reference_now, n=5000):
    events = []
    customer_ids = []
    reg_start = reference_now - timedelta(days=3 * 365)
    reg_end = reference_now - timedelta(days=30)
    for _ in range(n):
        cid = seeded_uuid()
        reg_date = fake.date_time_between(start_date=reg_start, end_date=reg_end)
        after = {
            "customer_id": cid, "email": fake.unique.email(),
            "first_name": fake.first_name(), "last_name": fake.last_name(),
            "phone": fake.msisdn()[:12], "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "gender": random.choice(["M", "F", "non_binary", "prefer_not_to_say"]),
            "registration_date": reg_date.isoformat(),
            "loyalty_tier": random.choices(LOYALTY_TIERS, weights=[50, 30, 15, 5])[0],
            "address_line1": fake.street_address(), "address_line2": "",
            "city": fake.city(), "state": fake.state_abbr(), "zip_code": fake.zipcode(),
            "country": "US", "is_active": True, "updated_at": reg_date.isoformat(),
        }
        events.append(cdc_event("c", after, event_ts=reg_date))
        customer_ids.append(cid)
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["customers"], "customers_batch0.json"), events)
    return customer_ids
 
 
def generate_orders_and_items(batch_dir, customer_ids, product_ids, product_skus, reference_now, n_orders=20000):
    order_events, item_events = [], []
    order_ids = []
    order_customer, order_status = {}, {}
    order_start = reference_now - timedelta(days=365)
 
    for _ in range(n_orders):
        oid = seeded_uuid()
        cid = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date=order_start, end_date=reference_now)
        final_status = random.choices(
            ORDER_STATUS_FLOW[1:] + ORDER_STATUS_TERMINAL_BRANCHES,
            weights=[10, 15, 55, 10, 10],
        )[0]
        discount_pct = random.choice([0, 0, 0, 0.1, 0.15, 0.2])
        n_items = random.choices([1, 2, 3, 4], weights=[40, 35, 18, 7])[0]
 
        line_total_sum = 0
        items_for_order = []
        for _ in range(n_items):
            pid = random.choice(product_ids)
            qty = random.choices([1, 2, 3], weights=[70, 22, 8])[0]
            unit_price = round(random.uniform(35, 180), 2)
            line_total = round(unit_price * qty, 2)
            line_total_sum += line_total
            items_for_order.append((pid, qty, unit_price, line_total))
 
        discount_amount = round(line_total_sum * discount_pct, 2) if discount_pct else None
        total_amount = round(line_total_sum - (discount_amount or 0), 2)
 
        order_after = {
            "order_id": oid, "customer_id": cid, "order_status": final_status,
            "order_date": order_date.isoformat(), "updated_at": order_date.isoformat(),
            "shipping_address_id": cid, "shipping_city": fake.city(),
            "shipping_state": fake.state_abbr(), "shipping_country": "US",
            "payment_method": random.choice(PAYMENT_METHODS),
            "discount_code": fake.lexify("SAVE???").upper() if discount_pct else None,
            "discount_amount": discount_amount, "total_amount": total_amount,
        }
        order_events.append(cdc_event("c", order_after, event_ts=order_date))
        order_ids.append(oid)
        order_customer[oid] = cid
        order_status[oid] = final_status
 
        for pid, qty, unit_price, line_total in items_for_order:
            item_id = seeded_uuid()
            return_requested = final_status == "returned"
            item_after = {
                "order_item_id": item_id, "order_id": oid, "product_id": pid,
                "sku": product_skus[pid], "quantity": qty, "unit_price": unit_price,
                "line_total": line_total, "return_requested": return_requested,
                "return_reason": random.choice(RETURN_REASONS) if return_requested else None,
            }
            item_events.append(cdc_event("c", item_after, event_ts=order_date))
 
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["orders"], "orders_batch0.json"), order_events)
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["order_items"], "order_items_batch0.json"), item_events)
    return order_ids, order_customer, order_status
 
 
def generate_clickstream(batch_dir, customer_ids, product_ids, order_ids, reference_now, n_events=180000, batch_num=0):
    events = []
    event_start = reference_now - timedelta(days=365)
    for _ in range(n_events):
        event_time = fake.date_time_between(start_date=event_start, end_date=reference_now)
        event_type = random.choices(EVENT_TYPES, weights=[35, 25, 12, 4, 6, 8, 10])[0]
        known_customer = random.random() < 0.55
        event = {
            "event_id": seeded_uuid(), "session_id": seeded_uuid(),
            "customer_id": random.choice(customer_ids) if known_customer else None,
            "event_type": event_type, "event_timestamp": event_time.isoformat(),
            "product_id": random.choice(product_ids) if event_type in ("product_view", "add_to_cart") else None,
            "page_url": f"/{fake.uri_path()}", "referrer": random.choice(REFERRERS),
            "device_type": random.choice(DEVICE_TYPES),
            "search_term": fake.word() if event_type == "search" else None,
            "order_id": random.choice(order_ids) if event_type == "purchase" else None,
        }
        events.append(event)
    fname = f"clickstream_batch{batch_num}_{seeded_short_id()}.json"
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["clickstream"], fname), events)
 
 
def generate_inventory(batch_dir, product_ids, product_skus, reference_now, days=30, batch_num=0):
    rows = []
    end_date = reference_now.date()
    for day_offset in range(days):
        snapshot_date = end_date - timedelta(days=day_offset)
        for pid in product_ids:
            for wh in WAREHOUSES:
                on_hand = random.randint(0, 150)
                reserved = random.randint(0, min(30, on_hand))
                rows.append({
                    "snapshot_id": seeded_uuid(), "snapshot_date": snapshot_date.isoformat(),
                    "product_id": pid, "sku": product_skus[pid], "warehouse_id": wh,
                    "quantity_on_hand": on_hand, "quantity_reserved": reserved,
                    "quantity_available": on_hand - reserved,
                    "reorder_point": 20, "days_of_supply": random.randint(0, 45),
                })
    fname = f"inventory_batch{batch_num}_{seeded_short_id()}.csv"
    write_csv(os.path.join(batch_dir, SUBFOLDERS["inventory"], fname), list(rows[0].keys()), rows)
 
 
def generate_batch_0(output_dir, state, reference_now, scale):
    batch_dir = os.path.join(output_dir, "batch_0")
    print("Generating batch_0 — initial full snapshot...")
 
    category_ids = generate_categories(output_dir, batch_dir)
    product_ids, product_skus, products_by_id = generate_products(
        batch_dir, category_ids, reference_now, n=scale["n_products"])
    customer_ids = generate_customers(batch_dir, reference_now, n=scale["n_customers"])
    order_ids, order_customer, order_status = generate_orders_and_items(
        batch_dir, customer_ids, product_ids, product_skus, reference_now, n_orders=scale["n_orders"])
    generate_clickstream(batch_dir, customer_ids, product_ids, order_ids, reference_now,
                          n_events=scale["n_clickstream"], batch_num=0)
    generate_inventory(batch_dir, product_ids, product_skus, reference_now,
                        days=scale["n_inventory_days"], batch_num=0)
 
    state["category_ids"] = category_ids
    state["product_ids"] = product_ids
    state["product_skus"] = product_skus
    state["products_by_id"] = products_by_id
    state["customer_ids"] = customer_ids
    state["order_ids"] = order_ids
    state["order_customer"] = order_customer
    state["order_status"] = order_status
    state["next_batch"] = 1
    print(f"batch_0 complete: {len(customer_ids)} customers, {len(order_ids)} orders, "
          f"{len(product_ids)} products written to {batch_dir}")
    return state
 
 
# ---------------------------------------------------------------------------
# Incremental batches (batch_1 onward) — new rows + a handful of update events
# ---------------------------------------------------------------------------
 
def generate_incremental_batch(output_dir, state, batch_num, reference_now, scale):
    batch_dir = os.path.join(output_dir, f"batch_{batch_num}")
    print(f"Generating batch_{batch_num} — incremental...")
 
    n_new_orders = scale["n_new_orders"]
    n_new_customers = scale["n_new_customers"]
    n_status_updates = scale["n_status_updates"]
    n_profile_updates = scale["n_profile_updates"]
    n_new_products = scale["n_new_products"]
    n_product_updates = scale["n_product_updates"]
 
    customer_ids = state["customer_ids"]
    product_ids = state["product_ids"]
    product_skus = {k: v for k, v in state["product_skus"].items()}
    products_by_id = {k: dict(v) for k, v in state.get("products_by_id", {}).items()}
    category_ids = state["category_ids"]
    order_ids = state["order_ids"]
    order_status = state["order_status"]
 
    # Product changes: a small number of new launches, plus price/discontinuation
    # updates on existing products. products and categories are file-based snapshot
    # sources (Silver upserts by key, see silver_files.py) — categories genuinely
    # don't change often for a business like this, so we don't force fake category
    # churn just to exercise a code path. Products realistically do change — new
    # launches, price changes, discontinuations — so those get generated here.
    new_product_rows = []
    for _ in range(n_new_products):
        pid = seeded_uuid()
        brand = random.choice(BRANDS)
        size = round(random.uniform(3.0, 14.0) * 2) / 2
        colour = random.choice(COLOURS)
        sku = f"{brand[:2].upper()}-{fake.lexify('????').upper()}-{size}-{colour[:3].upper()}"
        cost = round(random.uniform(15, 90), 2)
        retail = round(cost * random.uniform(1.4, 2.2), 2)
        row = {
            "product_id": pid, "sku": sku, "product_name": f"{brand} {fake.word().capitalize()}",
            "brand": brand, "category_id": random.choice(category_ids),
            "gender_target": random.choice(GENDER_TARGETS), "size_uk": size, "colour": colour,
            "material": random.choice(MATERIALS), "cost_price": cost, "retail_price": retail,
            "is_active": True, "launch_date": reference_now.date().isoformat(),
        }
        new_product_rows.append(row)
        product_ids.append(pid)
        product_skus[pid] = sku
        products_by_id[pid] = row
 
    updated_product_rows = []
    if products_by_id:
        existing_ids = list(products_by_id.keys())
        for pid in random.sample(existing_ids, min(n_product_updates, len(existing_ids))):
            row = dict(products_by_id[pid])
            if random.random() < 0.7:
                row["retail_price"] = round(row["retail_price"] * random.uniform(0.85, 1.15), 2)
            else:
                row["is_active"] = False
            products_by_id[pid] = row
            updated_product_rows.append(row)
 
    all_product_changes = new_product_rows + updated_product_rows
    if all_product_changes:
        write_csv(
            os.path.join(batch_dir, SUBFOLDERS["products"], f"products_batch{batch_num}.csv"),
            list(all_product_changes[0].keys()),
            all_product_changes,
        )
 
    # New customers
    new_customer_events = []
    new_customer_ids = []
    for _ in range(n_new_customers):
        cid = seeded_uuid()
        reg_date = reference_now
        after = {
            "customer_id": cid, "email": fake.unique.email(), "first_name": fake.first_name(),
            "last_name": fake.last_name(), "phone": fake.msisdn()[:12],
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "gender": random.choice(["M", "F", "non_binary", "prefer_not_to_say"]),
            "registration_date": reg_date.isoformat(),
            "loyalty_tier": "bronze", "address_line1": fake.street_address(), "address_line2": "",
            "city": fake.city(), "state": fake.state_abbr(), "zip_code": fake.zipcode(),
            "country": "US", "is_active": True, "updated_at": reg_date.isoformat(),
        }
        new_customer_events.append(cdc_event("c", after, event_ts=reg_date))
        new_customer_ids.append(cid)
 
    # Profile update events on existing customers (loyalty tier bump, address change)
    update_customer_events = []
    for cid in random.sample(customer_ids, min(n_profile_updates, len(customer_ids))):
        update_customer_events.append(cdc_event("u", {
            "customer_id": cid, "loyalty_tier": random.choice(LOYALTY_TIERS),
            "updated_at": reference_now.isoformat(),
        }, event_ts=reference_now))
 
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["customers"], f"customers_batch{batch_num}.json"),
                      new_customer_events + update_customer_events)
 
    # New orders (against existing + new customers) + their items
    all_customers_for_new_orders = customer_ids + new_customer_ids
    order_events, item_events = [], []
    new_order_ids = []
    for _ in range(n_new_orders):
        oid = seeded_uuid()
        cid = random.choice(all_customers_for_new_orders)
        order_date = reference_now
 
        items_for_order = []
        for _ in range(random.choices([1, 2, 3], weights=[40, 35, 25])[0]):
            pid = random.choice(product_ids)
            qty = random.randint(1, 2)
            unit_price = round(random.uniform(35, 180), 2)
            line_total = round(unit_price * qty, 2)
            items_for_order.append((pid, qty, unit_price, line_total))
        order_total = round(sum(li[3] for li in items_for_order), 2)
 
        after = {
            "order_id": oid, "customer_id": cid, "order_status": "pending",
            "order_date": order_date.isoformat(), "updated_at": order_date.isoformat(),
            "shipping_address_id": cid, "shipping_city": fake.city(),
            "shipping_state": fake.state_abbr(), "shipping_country": "US",
            "payment_method": random.choice(PAYMENT_METHODS),
            "discount_code": None, "discount_amount": None,
            "total_amount": order_total,
        }
        order_events.append(cdc_event("c", after, event_ts=order_date))
        new_order_ids.append(oid)
        order_status[oid] = "pending"
 
        for pid, qty, unit_price, line_total in items_for_order:
            item_events.append(cdc_event("c", {
                "order_item_id": seeded_uuid(), "order_id": oid, "product_id": pid,
                "sku": product_skus[pid], "quantity": qty,
                "unit_price": unit_price, "line_total": line_total,
                "return_requested": False, "return_reason": None,
            }, event_ts=order_date))
 
    # Status-progression update events on existing orders. days_offset spreads
    # these across a plausible 1-7 day fulfillment window relative to reference_now,
    # rather than always landing exactly on reference_now — see GENERATOR_GUIDE.md
    # for why a single wall-clock "now" made every SLA look either instant or
    # wildly late with no realistic middle ground.
    status_update_events = []
    candidates = [oid for oid, st in order_status.items() if st in ("pending", "confirmed", "shipped")]
    for oid in random.sample(candidates, min(n_status_updates, len(candidates))):
        current = order_status[oid]
        next_status = ORDER_STATUS_FLOW[min(ORDER_STATUS_FLOW.index(current) + 1, len(ORDER_STATUS_FLOW) - 1)] \
            if current in ORDER_STATUS_FLOW else current
        update_ts = reference_now - timedelta(days=random.randint(0, 6), hours=random.randint(0, 23))
        status_update_events.append(cdc_event("u", {
            "order_id": oid, "order_status": next_status,
            "updated_at": update_ts.isoformat(),
        }, event_ts=update_ts))
        order_status[oid] = next_status
 
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["orders"], f"orders_batch{batch_num}.json"),
                      order_events + status_update_events)
    write_json_lines(os.path.join(batch_dir, SUBFOLDERS["order_items"], f"order_items_batch{batch_num}.json"),
                      item_events)
 
    # A day's worth of new clickstream + one new inventory snapshot day
    generate_clickstream(batch_dir, all_customers_for_new_orders, product_ids, order_ids + new_order_ids,
                          reference_now, n_events=scale["n_clickstream_incremental"], batch_num=batch_num)
    generate_inventory(batch_dir, product_ids, product_skus, reference_now, days=1, batch_num=batch_num)
 
    state["customer_ids"] = customer_ids + new_customer_ids
    state["order_ids"] = order_ids + new_order_ids
    state["order_status"] = order_status
    state["product_ids"] = product_ids
    state["product_skus"] = product_skus
    state["products_by_id"] = products_by_id
    state["next_batch"] = batch_num + 1
    print(f"batch_{batch_num} complete: {n_new_customers} new customers, {n_new_orders} new orders, "
          f"{len(status_update_events)} status updates, {len(update_customer_events)} profile updates, "
          f"{len(new_product_rows)} new products, {len(updated_product_rows)} product updates "
          f"(categories: unchanged, by design — see docstring)")
    return state
 
 
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
 
def parse_as_of(value):
    """Accepts 'YYYY-MM-DD' or a full ISO datetime. Always returned as UTC."""
    if value is None:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise SystemExit(f"--as-of value '{value}' is not a valid ISO date/datetime, e.g. 2026-01-01")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
 
 
def main():
    parser = argparse.ArgumentParser(description="StepRight batch-based data generator")
    parser.add_argument("--batch", type=int, required=True, help="Batch number: 0 for initial full snapshot, 1+ for incremental")
    parser.add_argument("--output-dir", default="./raw_sources", help="Local directory to write generated files into")
    parser.add_argument("--state-file", default="./generator_state.json", help="Path to the state file tracking IDs across batches")
    parser.add_argument("--seed", type=int, default=None,
                         help="Random seed. Required (along with --as-of) for deterministic, reproducible batches.")
    parser.add_argument("--as-of", default=None,
                         help="Fixed reference date/time (ISO format, e.g. 2026-01-01), for deterministic batches. "
                              "Omit for dev/UAT batches — defaults to real current time.")
 
    # Scale — defaults match the locked dev/UAT scale. Override with small values
    # for integration-test batches (see GENERATOR_GUIDE.md).
    parser.add_argument("--n-customers", type=int, default=5000, help="batch_0 only")
    parser.add_argument("--n-orders", type=int, default=20000, help="batch_0 only")
    parser.add_argument("--n-products", type=int, default=500, help="batch_0 only")
    parser.add_argument("--n-clickstream", type=int, default=180000, help="batch_0 only")
    parser.add_argument("--n-inventory-days", type=int, default=30, help="batch_0 only")
    parser.add_argument("--n-new-orders", type=int, default=200, help="incremental batches only")
    parser.add_argument("--n-new-customers", type=int, default=50, help="incremental batches only")
    parser.add_argument("--n-status-updates", type=int, default=150, help="incremental batches only")
    parser.add_argument("--n-profile-updates", type=int, default=50, help="incremental batches only")
    parser.add_argument("--n-new-products", type=int, default=3, help="incremental batches only")
    parser.add_argument("--n-product-updates", type=int, default=10, help="incremental batches only")
    parser.add_argument("--n-clickstream-incremental", type=int, default=5000, help="incremental batches only")
 
    args = parser.parse_args()
 
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
 
    reference_now = parse_as_of(args.as_of)
    if args.as_of is None:
        print("No --as-of given — using real current time. This batch will NOT be reproducible run to run.")
    if args.seed is None:
        print("No --seed given — this batch will NOT be reproducible run to run.")
 
    scale = {
        "n_customers": args.n_customers, "n_orders": args.n_orders, "n_products": args.n_products,
        "n_clickstream": args.n_clickstream, "n_inventory_days": args.n_inventory_days,
        "n_new_orders": args.n_new_orders, "n_new_customers": args.n_new_customers,
        "n_status_updates": args.n_status_updates, "n_profile_updates": args.n_profile_updates,
        "n_new_products": args.n_new_products, "n_product_updates": args.n_product_updates,
        "n_clickstream_incremental": args.n_clickstream_incremental,
    }
 
    state = load_state(args.state_file)
 
    if args.batch == 0:
        state = generate_batch_0(args.output_dir, state, reference_now, scale)
    else:
        if not state.get("customer_ids"):
            raise SystemExit(f"No state found at {args.state_file} — run --batch 0 first before generating incremental batches.")
        state = generate_incremental_batch(args.output_dir, state, args.batch, reference_now, scale)
 
    save_state(args.state_file, state)
 
    print(f"\nBatch {args.batch} written locally to {os.path.join(args.output_dir, f'batch_{args.batch}')}")
    print("Next: manually upload this batch folder into the dev.stepright.staging volume, "
          "then run setup/02_data_loader.py.")
 
 
if __name__ == "__main__":
    main()
 