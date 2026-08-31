#!/usr/bin/env python3
"""Generates this repo's committed sample_data/ NDJSON files - the raw data the six example
Bronze pipelines actually ingest (see README's Getting started section for how to upload it).

Deterministic (fixed seed) so re-running this without changing anything reproduces the exact
committed files - only rerun it if you're deliberately changing the data shape/volume.

Includes a small number of DELIBERATELY invalid rows (marked below) that each source's Silver
pipeline is designed to catch - see silver_sales_etl/transformations/{sap,webshop}_orders.py's
REJECT_REASON_EXPR/WARNING_REASON_EXPR checks and silver_customers_etl/transformations/
customers.py's region check. Don't "fix" these rows; they're there on purpose to prove the
data-quality rules actually work.

One JSON object per line (NDJSON), not a JSON array: every bronze pipeline reads via
`cloudFiles.format: json` with no `multiLine` option set, so Auto Loader expects one record per
line.

Usage: uv run sample_data/generate_sample_data.py
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

SEED = 42
random.seed(SEED)
Faker.seed(SEED)

ROOT = Path(__file__).parent

# The full customer pool - CRM covers all of it; Finance only covers 2/3 (see
# generate_finance_credit_scores) so silver_customers_etl's LEFT JOIN enrichment has real,
# genuine nulls to handle, not just a hypothetical case. SAP/Webshop orders reference this same
# pool too, not a disjoint one, so the whole sample dataset is referentially consistent.
N_CUSTOMERS = 50
CUSTOMER_IDS = [f"CUST-{i:05d}" for i in range(1, N_CUSTOMERS + 1)]

# The full product pool (MDM) - SAP/Webshop orders reference these same product_id values (see
# generate_orders), same referential-consistency pattern as CUSTOMER_IDS.
N_PRODUCTS = 20
PRODUCT_IDS = [f"PROD-{i:05d}" for i in range(1, N_PRODUCTS + 1)]

# (product_name, subcategory, reference_price) triples, keyed by category - name, subcategory,
# and price are all picked together per category (see generate_mdm_products), never
# independently, so all levels actually agree (a "Smartwatch" is never filed under "Cookware",
# and never priced like a "Sticky Notes Pack" either). NOT from fake.catch_phrase() independent
# of category, which produced nonsense like "Office Supplies" /
# "Extended solution-oriented methodology" before this fix. Base Faker has no category-aware
# product-name provider, so this is hand-built rather than pulled in as a new dependency for a
# ~20-row reference dataset.
#
# reference_price is NOT a products.json (MDM) field - unit_price was deliberately dropped from
# the MDM schema/Silver table, since price is Product Management's ("prodman") recommendation,
# not MDM's own attribute (see bronze_prodman_etl and silver_products_etl's docstrings for why
# they're two separate Tier 1 sources joined at Silver, same pattern as CRM+Finance). This same
# per-product reference_price value drives BOTH generate_prodman_pricing (the actual
# product_pricing.json content) and generate_orders (so each order's amount is plausible for the
# product actually ordered) - one source of truth, two uses, not duplicated by hand.
PRODUCTS_BY_CATEGORY = {
    "Electronics": [
        ("Wireless Headphones", "Audio", 79.99),
        ("Bluetooth Speaker", "Audio", 49.99),
        ("Noise-Cancelling Earbuds", "Audio", 129.99),
        ("4K Monitor", "Computer Accessories", 299.99),
        ("Mechanical Keyboard", "Computer Accessories", 89.99),
        ("Smartwatch", "Wearables", 199.99),
        ("Portable Charger", "Charging & Power", 29.99),
        ("USB-C Hub", "Charging & Power", 34.99),
    ],
    "Home & Kitchen": [
        ("Stainless Steel Blender", "Small Appliances", 59.99),
        ("Air Fryer", "Small Appliances", 89.99),
        ("Electric Kettle", "Small Appliances", 34.99),
        ("Vacuum Sealer", "Small Appliances", 54.99),
        ("Non-Stick Frying Pan", "Cookware", 24.99),
        ("Cast Iron Skillet", "Cookware", 39.99),
        ("Ceramic Coffee Mug", "Drinkware", 9.99),
        ("Cutting Board Set", "Kitchen Tools", 19.99),
    ],
    "Apparel": [
        ("Cotton T-Shirt", "Tops", 14.99),
        ("Wool Sweater", "Tops", 49.99),
        ("Yoga Pants", "Bottoms", 34.99),
        ("Denim Jacket", "Outerwear", 69.99),
        ("Rain Jacket", "Outerwear", 59.99),
        ("Running Shoes", "Footwear", 79.99),
        ("Leather Belt", "Accessories", 24.99),
        ("Baseball Cap", "Accessories", 17.99),
    ],
    "Sports & Outdoors": [
        ("Yoga Mat", "Fitness", 24.99),
        ("Resistance Bands", "Fitness", 14.99),
        ("Camping Tent", "Camping", 149.99),
        ("Hiking Backpack", "Camping", 79.99),
        ("Sleeping Bag", "Camping", 59.99),
        ("Trekking Poles", "Hiking", 34.99),
        ("Bicycle Helmet", "Cycling", 44.99),
        ("Insulated Water Bottle", "Hydration", 19.99),
    ],
    "Office Supplies": [
        ("Mechanical Pencil Set", "Writing", 9.99),
        ("Sticky Notes Pack", "Writing", 6.99),
        ("Whiteboard Marker Set", "Writing", 12.99),
        ("Desk Organizer", "Desk Accessories", 19.99),
        ("Heavy-Duty Stapler", "Desk Accessories", 14.99),
        ("Ergonomic Office Chair", "Furniture", 199.99),
        ("Adjustable Laptop Stand", "Furniture", 39.99),
        ("File Folder Set", "Filing & Storage", 11.99),
    ],
}
PRODUCT_CATEGORIES = tuple(PRODUCTS_BY_CATEGORY)

VALID_ORDER_STATUSES = ("pending", "completed", "cancelled", "refunded")
VALID_CRM_REGIONS = ("AMER", "EMEA", "APAC")

# (country, Faker locale) pairs, one per region - locale drives city/state/postal_code/name/
# phone so they're actually consistent with the assigned country, not just the country string
# on its own. Every locale below was checked live (not guessed) to confirm it has real address
# data instead of silently falling back to Faker's US default - e.g. "ar_AE" looked plausible
# but actually returned US-style cities/states, so UAE was dropped in favor of Italy; Singapore
# has no dedicated Faker locale at all, dropped in favor of Indonesia.
REGION_COUNTRIES = {
    "AMER": [
        ("United States", "en_US"),
        ("Canada", "en_CA"),
        ("Brazil", "pt_BR"),
        ("Mexico", "es_MX"),
    ],
    "EMEA": [
        ("Germany", "de_DE"),
        ("United Kingdom", "en_GB"),
        ("France", "fr_FR"),
        ("Italy", "it_IT"),
    ],
    "APAC": [
        ("Japan", "ja_JP"),
        ("Australia", "en_AU"),
        ("Indonesia", "id_ID"),
        ("India", "en_IN"),
    ],
}
# Used only for the deliberately-invalid-region CRM row (see generate_crm_customers).
INVALID_REGION_COUNTRY = ("Argentina", "es_AR")

# RFC 2606-reserved, guaranteed to never resolve to a real mailbox.
EMAIL_DOMAINS = ("example.com", "example.org", "example.net")

# ITU country calling codes (no leading +) - only included on ~half the CRM phone numbers on
# purpose (see generate_crm_customers): real-world Bronze data is messy like this, and Silver/
# downstream cleanup normalizing phone numbers is meant to be a thing this sample data can
# actually demonstrate, not something to paper over here.
COUNTRY_CALLING_CODES = {
    "United States": "1",
    "Canada": "1",
    "Brazil": "55",
    "Mexico": "52",
    "Germany": "49",
    "United Kingdom": "44",
    "France": "33",
    "Italy": "39",
    "Japan": "81",
    "Australia": "61",
    "Indonesia": "62",
    "India": "91",
    "Argentina": "54",
}

SHIPPING_REGIONS = ("US-EAST", "US-WEST", "EU-WEST", "EU-CENTRAL", "APAC-EAST")

_locale_fakers: dict[str, Faker] = {}


def _faker_for(locale: str) -> Faker:
    """One cached, seeded Faker instance per locale - not a new instance per row (wasteful),
    and not a single shared en_US instance (which is what produced wrong city/state/postal
    values for non-US customers before this fix)."""
    if locale not in _locale_fakers:
        f = Faker(locale)
        f.seed_instance(SEED)
        _locale_fakers[locale] = f
    return _locale_fakers[locale]


def _write_ndjson(rows: list[dict], relative_path: str) -> None:
    path = ROOT / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"wrote {len(rows)} rows -> sample_data/{relative_path}")


def _random_recent_date(days_back: int = 60) -> str:
    return (datetime.now() - timedelta(days=random.randint(0, days_back))).date().isoformat()


def _random_recent_timestamp(days_back: int = 60) -> str:
    dt = datetime.now() - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return dt.isoformat()


def generate_orders(order_id_prefix: str, n_orders: int, product_prices: dict[str, float]) -> list[dict]:
    """Shared row shape for SAP and Webshop orders - same fields, different order_id prefix.

    amount is derived from quantity * the ordered product's reference_price (see
    PRODUCTS_BY_CATEGORY), with +-10% variance for realism - not an independent random value
    disconnected from what was actually ordered.

    Last two rows are deliberately invalid: one negative amount (violates Silver's
    non_positive_amount reject check - excluded from the Silver table, landing in
    silver_data_quality instead; overrides the plausible-amount calculation on purpose, since
    the point is an impossible value, not an implausible-but-positive one), one unknown status
    (violates the soft unknown_status warning check - still merges into the Silver table, but
    also flagged in silver_data_quality; amount stays plausible here, only status is
    deliberately wrong).
    """
    rows = []
    for i in range(1, n_orders + 1):
        product_id = random.choice(PRODUCT_IDS)
        quantity = random.randint(1, 5)
        amount = round(product_prices[product_id] * quantity * random.uniform(0.9, 1.1), 2)
        status = random.choice(VALID_ORDER_STATUSES)

        if i == n_orders - 1:
            amount = -12.50
        elif i == n_orders:
            status = "returned"

        rows.append(
            {
                "order_id": f"{order_id_prefix}-{i:05d}",
                "customer_id": random.choice(CUSTOMER_IDS),
                "product_id": product_id,
                "order_date": _random_recent_date(),
                "quantity": quantity,
                "amount": amount,
                "status": status,
                "shipping_region": random.choice(SHIPPING_REGIONS),
            }
        )
    return rows


def generate_crm_customers() -> list[dict]:
    """One row per customer_id in the shared pool. Last row is deliberately invalid: a region
    outside AMER/EMEA/APAC, silently dropped by Silver's hard region filter.

    city/state_province/postal_code/name/phone all come from ONE locale-appropriate Faker
    instance per row (see _faker_for), so a German customer gets a German city/postcode, not
    Faker's US default. Not every country has a real "state/province" concept in Faker (e.g.
    en_GB, fr_FR, ja_JP have no state() provider) - state_province is left None there rather
    than filled with a wrong/fabricated value, which is more correct, not less complete.

    address_line1 and phone get extra per-country handling beyond just picking the right
    locale, because Faker's own output wasn't reliable enough as-is (confirmed live):
    - ja_JP's street_address() produces broken output (a Japanese surname spliced into an
      English "... Street" template, not a real Japanese address at all) - built from the
      actual Japan block/lot components (town/chome/ban/gou) instead, which do work correctly
      and combine into a genuine Japanese address format.
    - phone doesn't use fake.phone_number() at all - confirmed live that some locales'
      provider (es_MX in particular) randomly fabricates a bogus leading "+1"/"1-" NANP-style
      prefix that looks like a real country code but isn't one. Built from a plain
      digit-grouped pattern instead, with COUNTRY_CALLING_CODES prepended on only ~half of
      rows (random) and left off the rest ON PURPOSE - real Bronze data is this messy, and
      it's meant to give Silver/downstream cleanup something real to normalize, not be
      pre-cleaned here.
    """
    rows = []
    for idx, customer_id in enumerate(CUSTOMER_IDS, start=1):
        region = random.choice(VALID_CRM_REGIONS)
        if idx == len(CUSTOMER_IDS):
            region = "LATAM"
            country, locale = INVALID_REGION_COUNTRY
        else:
            country, locale = random.choice(REGION_COUNTRIES[region])

        fake = _faker_for(locale)
        try:
            state_province = fake.state()
        except AttributeError:
            state_province = None

        if country == "Japan":
            address_line1 = f"{fake.town()}{fake.chome()}{fake.ban()}{fake.gou()}"
        else:
            address_line1 = fake.street_address()
        address_line1 = address_line1.replace("\n", ", ")  # keep it a single line always

        local_number = fake.numerify("#### ####")
        phone = f"+{COUNTRY_CALLING_CODES[country]} {local_number}" if random.random() < 0.5 else local_number

        email = f"{fake.user_name()}@{random.choice(EMAIL_DOMAINS)}"

        rows.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": email,
                "phone": phone,
                "address_line1": address_line1,
                "city": fake.city(),
                "state_province": state_province,
                "postal_code": fake.postcode(),
                "country": country,
                "region": region,
                "updated_at": _random_recent_timestamp(),
            }
        )
    return rows


def generate_finance_credit_scores() -> list[dict]:
    """Only 2/3 of CUSTOMER_IDS, deliberately - a random (not first-N, to avoid an obvious
    pattern) subset, not the full CRM pool. Real customers don't all have a credit score on
    file yet; this gives silver_customers_etl's customers_from_crm LEFT JOIN against
    _latest_credit_score() a genuine null case to handle for the missing third, instead of
    every row happening to enrich cleanly.
    """
    covered = sorted(random.sample(CUSTOMER_IDS, k=round(len(CUSTOMER_IDS) * 2 / 3)))
    return [
        {
            "customer_id": customer_id,
            "credit_score": random.randint(300, 850),
            "updated_at": _random_recent_timestamp(),
        }
        for customer_id in covered
    ]


def generate_mdm_products() -> tuple[list[dict], dict[str, float]]:
    """The full product pool - see PRODUCT_IDS above for why. category is picked first, then
    (product_name, subcategory, reference_price) come together from that category's own list
    (PRODUCTS_BY_CATEGORY) - never picked independently of category or of each other.

    Returns (rows, product_prices) - product_prices maps product_id -> reference_price, for
    generate_orders (order amount plausibility) and generate_prodman_pricing (prodman's own
    file) to both use. reference_price itself is NEVER written into these MDM rows (see
    PRODUCTS_BY_CATEGORY's comment for why - it's prodman's field, not MDM's).
    """
    rows = []
    product_prices = {}
    for product_id in PRODUCT_IDS:
        category = random.choice(PRODUCT_CATEGORIES)
        product_name, subcategory, reference_price = random.choice(PRODUCTS_BY_CATEGORY[category])
        product_prices[product_id] = reference_price
        rows.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "subcategory": subcategory,
                "updated_at": _random_recent_timestamp(),
            }
        )
    return rows, product_prices


def generate_prodman_pricing(product_prices: dict[str, float]) -> list[dict]:
    """One row per product_id, product_id + reference_price only - Product Management's own
    minimal feed (see bronze_prodman_etl's docstring for why it carries no timestamp of its
    own). Uses the SAME product_prices mapping generate_mdm_products already produced - one
    source of truth for reference_price, not a second, independently-random one.
    """
    return [
        {"product_id": product_id, "reference_price": reference_price}
        for product_id, reference_price in product_prices.items()
    ]


def main() -> None:
    product_rows, product_prices = generate_mdm_products()
    _write_ndjson(generate_orders("SAP", 100, product_prices), "sap/orders/orders.json")
    _write_ndjson(generate_orders("WEB", 100, product_prices), "webshop/orders/orders.json")
    _write_ndjson(generate_crm_customers(), "crm/customers/customers.json")
    _write_ndjson(
        generate_finance_credit_scores(),
        "finance/customer_credit_score/customer_credit_score.json",
    )
    _write_ndjson(product_rows, "mdm/products/products.json")
    _write_ndjson(
        generate_prodman_pricing(product_prices),
        "prodman/product_pricing/product_pricing.json",
    )


if __name__ == "__main__":
    main()
