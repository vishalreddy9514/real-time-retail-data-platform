"""
reference_data.py

Generates synthetic (non-real) UK retail dimension data:
    - customers
    - products
    - stores

These are the "slowly changing" dimensions that the streaming transaction
events will reference by ID. In a real platform these would come from a
CRM / PIM / ERP system via CDC or batch extracts; here we simulate them
once and persist to /data/reference/*.csv so both the batch and streaming
pipelines can join against the same dimension keys.

No real personal data is used - names are generated from a fixed pool of
common UK first/last names combined randomly, purely for readability of
sample dashboards.

Why customer_id, product_id, and store_id are all sequential (not
random UUIDs): the data-generator container regenerates this reference
data on every restart. If IDs were random (e.g. uuid4()), every restart
would produce a brand-new set of IDs, while transactions already
streamed into Kafka/Snowflake would still reference the old, now
nonexistent set - breaking referential integrity between
STG_TRANSACTIONS and STG_CUSTOMERS/STG_PRODUCTS/STG_STORES on every
restart. Sequential IDs, generated in a fixed, seeded order, are stable
across restarts as long as the same n is used, which is what dbt's
relationship tests rely on.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "reference"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference lookup pools
# ---------------------------------------------------------------------------

UK_STORES = [
    ("London", "Greater London", 51.5074, -0.1278),
    ("Birmingham", "West Midlands", 52.4862, -1.8904),
    ("Manchester", "North West", 53.4808, -2.2426),
    ("Leeds", "Yorkshire", 53.8008, -1.5491),
    ("Liverpool", "North West", 53.4084, -2.9916),
    ("Bristol", "South West", 51.4545, -2.5879),
    ("Glasgow", "Scotland", 55.8642, -4.2518),
    ("Edinburgh", "Scotland", 55.9533, -3.1883),
    ("Sheffield", "Yorkshire", 53.3811, -1.4701),
    ("Nottingham", "East Midlands", 52.9548, -1.1581),
    ("Cardiff", "Wales", 51.4816, -3.1791),
    ("Southampton", "South East", 50.9097, -1.4044),
    ("Guildford", "South East", 51.2362, -0.5704),
]

CATEGORIES = {
    "Groceries": ["Bakery", "Dairy", "Fresh Produce", "Frozen", "Snacks"],
    "Electronics": ["Mobile Accessories", "Audio", "Small Appliances"],
    "Clothing": ["Menswear", "Womenswear", "Kidswear"],
    "Home & Garden": ["Kitchenware", "Furniture", "Garden Tools"],
    "Health & Beauty": ["Skincare", "Haircare", "Vitamins"],
}

BRANDS = ["Tesco Value", "AeroTech", "UrbanFit", "GreenLeaf", "NordicHome",
          "PureGlow", "Bristol Bakes", "MetroWear", "CasaViva", "TrueTone"]

FIRST_NAMES = ["Oliver", "Amelia", "George", "Isla", "Noah", "Ava", "Jack",
               "Freya", "Leo", "Grace", "Harry", "Sophie", "Muhammad", "Ella",
               "Arjun", "Priya", "Liam", "Chloe", "Ryan", "Megan"]
LAST_NAMES = ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson",
              "Evans", "Thomas", "Roberts", "Walker", "Khan", "Patel",
              "Murphy", "Clarke", "Hughes", "Edwards", "Green", "Hall"]

SEGMENTS = ["Budget", "Mainstream", "Premium", "Loyalty-Gold"]
AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]

POSTCODE_AREAS = ["GU", "SW", "M1", "B1", "LS1", "L1", "BS1", "G1", "EH1",
                   "S1", "NG1", "CF1", "SO1"]


@dataclass
class Customer:
    customer_id: str
    customer_name: str
    age_group: str
    postcode_area: str
    customer_segment: str
    registration_date: str


@dataclass
class Product:
    product_id: str
    product_name: str
    category: str
    subcategory: str
    brand: str
    price: float
    cost: float
    stock_quantity: int


@dataclass
class Store:
    store_id: str
    store_name: str
    city: str
    region: str
    latitude: float
    longitude: float


def generate_customers(n: int) -> list[Customer]:
    customers = []
    start = date(2018, 1, 1)
    span_days = (date.today() - start).days
    for i in range(n):
        reg_date = start + timedelta(days=random.randint(0, span_days))
        customers.append(
            Customer(
                customer_id=f"CUST-{i + 1:08d}",
                customer_name=f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                age_group=random.choice(AGE_GROUPS),
                postcode_area=random.choice(POSTCODE_AREAS),
                customer_segment=random.choices(
                    SEGMENTS, weights=[0.3, 0.4, 0.2, 0.1]
                )[0],
                registration_date=reg_date.isoformat(),
            )
        )
    return customers


def generate_products(n: int) -> list[Product]:
    products = []
    for i in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        cost = round(random.uniform(1.0, 150.0), 2)
        margin = random.uniform(1.15, 1.9)
        products.append(
            Product(
                product_id=f"PROD-{i+1:06d}",
                product_name=f"{subcategory} Item {i+1}",
                category=category,
                subcategory=subcategory,
                brand=random.choice(BRANDS),
                price=round(cost * margin, 2),
                cost=cost,
                stock_quantity=random.randint(0, 5000),
            )
        )
    return products


def generate_stores() -> list[Store]:
    stores = []
    for i, (city, region, lat, lon) in enumerate(UK_STORES):
        stores.append(
            Store(
                store_id=f"STORE-{i+1:03d}",
                store_name=f"{city} Superstore",
                city=city,
                region=region,
                latitude=lat,
                longitude=lon,
            )
        )
    return stores


def _write_csv(path: Path, rows: list) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for r in rows:
            writer.writerow(asdict(r))


def main(num_customers: int = 5000, num_products: int = 800):
    customers = generate_customers(num_customers)
    products = generate_products(num_products)
    stores = generate_stores()

    _write_csv(OUTPUT_DIR / "customers.csv", customers)
    _write_csv(OUTPUT_DIR / "products.csv", products)
    _write_csv(OUTPUT_DIR / "stores.csv", stores)

    print(f"Generated {len(customers)} customers -> {OUTPUT_DIR / 'customers.csv'}")
    print(f"Generated {len(products)} products   -> {OUTPUT_DIR / 'products.csv'}")
    print(f"Generated {len(stores)} stores      -> {OUTPUT_DIR / 'stores.csv'}")


if __name__ == "__main__":
    main()