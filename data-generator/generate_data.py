import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

PRODUCTS = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam", "Docking Station", "USB-C Hub"]
CATEGORIES = {"Laptop": "Computing", "Mouse": "Accessories", "Keyboard": "Accessories",
              "Monitor": "Computing", "Headset": "Audio", "Webcam": "Audio",
              "Docking Station": "Accessories", "USB-C Hub": "Accessories"}

def generate_customers(n=200):
    rows = []
    for i in range(n):
        rows.append({
            "customer_id": f"CUST-{i:04d}",
            "email": f"customer{i}@example.com" if random.random() > 0.02 else "",
            "country": random.choice(["FR", "DE", "ES", "IT", "BE"]),
            "signup_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d"),
        })
    return rows

def generate_products():
    return [{"product_id": f"PROD-{i:03d}", "name": p, "category": CATEGORIES[p],
              "price": round(random.uniform(15, 900), 2)} for i, p in enumerate(PRODUCTS)]

def generate_orders(customers, products, n=5000):
    rows = []
    for _ in range(n):
        order_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 270))
        row = {
            "order_id": str(uuid.uuid4()),
            "customer_id": random.choice(customers)["customer_id"],
            "product_id": random.choice(products)["product_id"],
            "quantity": random.randint(1, 5),
            "order_date": order_date.strftime("%Y-%m-%d"),
        }
        rows.append(row)
        if random.random() < 0.01:
            rows.append(row.copy())
    return rows

def write_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders(customers, products)

    write_csv(customers, "customers.csv")
    write_csv(products, "products.csv")
    write_csv(orders, "orders.csv")
    print(f"{len(customers)} customers, {len(products)} products, {len(orders)} orders générés")
