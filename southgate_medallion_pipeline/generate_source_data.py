"""
Generates 10 realistic retail + logistics source CSV files for the medallion pipeline.
Deterministic (seeded) so re-runs produce the same data.
Intentionally injects a small % of dirty records (nulls, bad emails, negative qty,
duplicate keys) so silver-layer quality rules and quarantine have something to catch.
"""
import csv, random, os
from datetime import datetime, timedelta

random.seed(42)
CATALOG = "retail_lakehouse"   # or whatever you passed to the setup notebooks
OUT = f"/Volumes/{CATALOG}/landing/source_files"
os.makedirs(OUT, exist_ok=True)

FIRST = ["Aiden","Bella","Chen","Divya","Ethan","Fatima","George","Hana","Isaac","Jia",
         "Kiran","Liam","Mei","Noah","Olivia","Priya","Quinn","Ravi","Sofia","Tom",
         "Uma","Vikram","Wendy","Xavier","Yara","Zane","Arjun","Grace","Hugo","Ines"]
LAST  = ["Nguyen","Smith","Patel","Wang","Brown","Singh","Taylor","Kumar","Lee","Wilson",
         "Chen","Sharma","Jones","Martin","Iyer","Thompson","Khan","White","Das","Harris"]
CITIES = [("Melbourne","VIC"),("Sydney","NSW"),("Brisbane","QLD"),("Perth","WA"),
          ("Adelaide","SA"),("Hobart","TAS"),("Canberra","ACT"),("Darwin","NT"),
          ("Geelong","VIC"),("Newcastle","NSW"),("Gold Coast","QLD"),("Wollongong","NSW")]
CATEGORIES = {
    "Electronics": ["4K TV","Bluetooth Speaker","Laptop","Tablet","Noise-Cancel Headphones","Smart Watch","Gaming Console","Wireless Router"],
    "Home & Garden": ["Cordless Drill","Garden Hose","LED Floor Lamp","Air Fryer","Robot Vacuum","Outdoor Heater","Tool Set","Pressure Washer"],
    "Grocery": ["Olive Oil 1L","Coffee Beans 500g","Protein Bars 12pk","Basmati Rice 5kg","Almond Milk 1L","Dark Chocolate 200g"],
    "Apparel": ["Running Shoes","Rain Jacket","Merino Jumper","Denim Jeans","Hi-Vis Vest","Work Boots"],
    "Sports": ["Yoga Mat","Dumbbell Set 20kg","Camping Tent 4P","Mountain Bike Helmet","Esky 40L"],
}
CARRIERS = ["AusFreight Express","StarTrack Logistics","Linfox Direct","Toll Priority","CouriersPlease","TNT Road"]
CHANNELS = ["ONLINE","STORE","MARKETPLACE","B2B"]
PAY_METHODS = ["CREDIT_CARD","DEBIT_CARD","PAYPAL","AFTERPAY","GIFT_CARD","BANK_TRANSFER"]
RETURN_REASONS = ["DAMAGED_IN_TRANSIT","WRONG_ITEM","CHANGE_OF_MIND","FAULTY","SIZE_ISSUE","LATE_DELIVERY"]

BASE = datetime(2026, 1, 1)
def rand_date(days_back_max=240, days_fwd=0):
    return BASE + timedelta(days=random.randint(-days_back_max, days_fwd),
                            hours=random.randint(0,23), minutes=random.randint(0,59))
def ts(d): return d.strftime("%Y-%m-%d %H:%M:%S")
def dt(d): return d.strftime("%Y-%m-%d")

def write(name, header, rows):
    entity = name.replace(".csv", "")
    folder = os.path.join(OUT, entity)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, name)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"{name:28s} {len(rows):>6,} records -> {path}")

# ---------------- 1. customers.csv (1,200 + dirty) ----------------
customers = []
for i in range(1, 1201):
    fn, ln = random.choice(FIRST), random.choice(LAST)
    city, state = random.choice(CITIES)
    email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
    if i % 97 == 0: email = "not-an-email"           # dirty: invalid email
    if i % 151 == 0: email = ""                       # dirty: null email
    customers.append([f"CUST{i:06d}", fn, ln, email,
                      f"+61 4{random.randint(10000000,99999999)}",
                      city, state, random.randint(3000, 7999),
                      random.choice(["GOLD","SILVER","BRONZE","NONE"]),
                      random.choice(CHANNELS), ts(rand_date(400))])
customers.append(customers[10][:])                    # dirty: exact duplicate PK
write("customers.csv",
      ["customer_id","first_name","last_name","email","phone","city","state","postcode",
       "loyalty_tier","preferred_channel","registered_at"], customers)

# ---------------- 2. products.csv (500) ----------------
products = []
pid = 0
while pid < 500:
    for cat, names in CATEGORIES.items():
        for n in names:
            pid += 1
            if pid > 500: break
            cost = round(random.uniform(4, 900), 2)
            price = round(cost * random.uniform(1.2, 2.4), 2)
            if pid % 73 == 0: price = -1.0            # dirty: negative price
            products.append([f"PROD{pid:05d}", f"{n} v{random.randint(1,5)}", cat,
                             random.choice(["Voltix","HomePro","TrailMax","DailyFresh","UrbanFit","CorePower"]),
                             cost, price, random.choice(["EA","PK","BOX"]),
                             random.choice(["ACTIVE","ACTIVE","ACTIVE","DISCONTINUED"]),
                             ts(rand_date(500))])
write("products.csv",
      ["product_id","product_name","category","brand","unit_cost","list_price","uom",
       "status","created_at"], products)

# ---------------- 3. stores.csv (60) ----------------
stores = []
for i in range(1, 61):
    city, state = random.choice(CITIES)
    stores.append([f"STOR{i:04d}", f"{city} {random.choice(['Central','North','South','East','West','Outlet'])}",
                   random.choice(["FLAGSHIP","STANDARD","EXPRESS","DARK_STORE"]),
                   city, state, round(random.uniform(400, 6500), 0),
                   dt(rand_date(3000)), random.choice(["OPEN","OPEN","OPEN","REFURB"])])
write("stores.csv",
      ["store_id","store_name","format","city","state","floor_area_sqm","opened_date","status"], stores)

# ---------------- 4. suppliers.csv (120) ----------------
suppliers = []
for i in range(1, 121):
    city, state = random.choice(CITIES)
    suppliers.append([f"SUPP{i:04d}",
                      f"{random.choice(['Pacific','Southern','Metro','National','Prime','Summit'])} {random.choice(['Trading','Distribution','Imports','Supplies','Wholesale'])} Pty Ltd",
                      random.choice(list(CATEGORIES.keys())), city, state,
                      random.randint(2, 45), round(random.uniform(80, 99.9), 1),
                      random.choice(["ACTIVE","ACTIVE","ON_HOLD"])])
write("suppliers.csv",
      ["supplier_id","supplier_name","primary_category","city","state","lead_time_days",
       "otif_score_pct","status"], suppliers)

# ---------------- 5. orders.csv (5,000 + dirty) ----------------
orders, order_ids = [], []
for i in range(1, 5001):
    oid = f"ORD{i:07d}"
    order_ids.append(oid)
    od = rand_date(240)
    cust = f"CUST{random.randint(1,1200):06d}"
    if i % 211 == 0: cust = "CUST999999"              # dirty: orphan customer FK
    orders.append([oid, cust, f"STOR{random.randint(1,60):04d}",
                   random.choice(CHANNELS), ts(od),
                   random.choice(["COMPLETED","COMPLETED","COMPLETED","CANCELLED","PENDING"]),
                   random.choice(["STANDARD","EXPRESS","CLICK_COLLECT"]),
                   round(random.uniform(0, 25), 2)])
write("orders.csv",
      ["order_id","customer_id","store_id","channel","order_ts","order_status",
       "fulfilment_type","shipping_fee"], orders)

# ---------------- 6. order_items.csv (~13,000 + dirty) ----------------
items = []
li = 0
for oid in order_ids:
    for line in range(1, random.randint(1, 5) + 1):
        li += 1
        qty = random.randint(1, 6)
        if li % 499 == 0: qty = -3                    # dirty: negative quantity
        price = round(random.uniform(5, 1200), 2)
        disc = round(price * qty * random.choice([0, 0, 0, 0.05, 0.1, 0.2]), 2)
        items.append([f"LINE{li:08d}", oid, line, f"PROD{random.randint(1,500):05d}",
                      qty, price, disc, round(price * qty - disc, 2)])
write("order_items.csv",
      ["order_line_id","order_id","line_number","product_id","quantity","unit_price",
       "discount_amount","line_total"], items)

# ---------------- 7. shipments.csv (4,200) ----------------
shipments = []
shipped_orders = random.sample(order_ids, 4200)
for i, oid in enumerate(shipped_orders, 1):
    ship = rand_date(230)
    transit = random.randint(1, 9)
    delivered = ship + timedelta(days=transit, hours=random.randint(1, 20))
    status = random.choice(["DELIVERED"]*7 + ["IN_TRANSIT","DELAYED","LOST"])
    shipments.append([f"SHIP{i:07d}", oid, random.choice(CARRIERS),
                      f"CON{random.randint(10**9, 10**10 - 1)}",
                      ts(ship), ts(delivered) if status == "DELIVERED" else "",
                      transit, round(random.uniform(0.2, 42.0), 1), status])
write("shipments.csv",
      ["shipment_id","order_id","carrier","consignment_no","shipped_ts","delivered_ts",
       "promised_transit_days","weight_kg","shipment_status"], shipments)

# ---------------- 8. inventory_snapshots.csv (3,000) ----------------
inv = []
for i in range(1, 3001):
    on_hand = random.randint(0, 800)
    inv.append([f"INV{i:07d}", dt(rand_date(30)), f"STOR{random.randint(1,60):04d}",
                f"PROD{random.randint(1,500):05d}", on_hand,
                random.randint(0, min(on_hand, 50)), random.randint(0, 200),
                random.randint(10, 120)])
write("inventory_snapshots.csv",
      ["snapshot_id","snapshot_date","store_id","product_id","qty_on_hand",
       "qty_reserved","qty_on_order","reorder_point"], inv)

# ---------------- 9. payments.csv (5,100 incl. splits) ----------------
payments = []
pi = 0
for oid in order_ids:
    n_pay = 1 if random.random() > 0.02 else 2        # occasional split payment
    for _ in range(n_pay):
        pi += 1
        payments.append([f"PAY{pi:07d}", oid, random.choice(PAY_METHODS),
                         round(random.uniform(10, 2400), 2),
                         random.choice(["SETTLED"]*8 + ["DECLINED","REFUNDED"]),
                         ts(rand_date(240)), f"AUTH{random.randint(10**7,10**8-1)}"])
write("payments.csv",
      ["payment_id","order_id","payment_method","amount","payment_status",
       "payment_ts","auth_code"], payments)

# ---------------- 10. returns.csv (420) ----------------
rets = []
for i in range(1, 421):
    rets.append([f"RET{i:06d}", random.choice(order_ids),
                 f"PROD{random.randint(1,500):05d}", random.randint(1, 3),
                 random.choice(RETURN_REASONS),
                 random.choice(["APPROVED","APPROVED","APPROVED","REJECTED","PENDING"]),
                 round(random.uniform(5, 900), 2), ts(rand_date(200))])
write("returns.csv",
      ["return_id","order_id","product_id","qty_returned","return_reason",
       "return_status","refund_amount","returned_ts"], rets)

print("\nAll 10 source files generated in", OUT)
