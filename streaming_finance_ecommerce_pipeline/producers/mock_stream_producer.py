"""
Mock Streaming Event Producer for Finance & E-Commerce

Generates high-velocity simulated event streams to feed Kafka topics or local tests:
1. Financial Transactions: Normal transactions interspersed with simulated fraud
   attacks (rapid velocity card bursts, sudden large amounts, international IP jumps,
   and high-risk merchant categories).
2. E-Commerce Clickstreams: Real-time user navigation (views, searches, cart additions,
   checkouts, cart abandonment) and product surges for dynamic pricing.
"""

import sys
import time
import json
import random
import uuid
from datetime import datetime, timezone
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MockProducer")


# ==============================================================================
# SEED DATA & CONSTANTS
# ==============================================================================
USERS = [f"USR-{1000 + i}" for i in range(1, 50)]
FRAUD_TARGET_USERS = [f"USR-999{i}" for i in range(1, 4)]  # Dedicated users to simulate fraud attacks

MERCHANTS = [
    ("MERCH-001", "Amazon Online", "retail", "US", "Seattle"),
    ("MERCH-002", "Whole Foods Market", "groceries", "US", "Austin"),
    ("MERCH-003", "Starbucks Coffee", "dining", "US", "Seattle"),
    ("MERCH-004", "Best Buy Electronics", "electronics", "US", "Richfield"),
    ("MERCH-005", "Binance Global", "crypto", "KY", "George Town"),
    ("MERCH-006", "Swiss Offshore Wire", "wire_transfer", "CH", "Zurich"),
    ("MERCH-007", "Vegas Palace Casino", "gambling", "US", "Las Vegas"),
    ("MERCH-008", "Target Stores", "retail", "US", "Minneapolis"),
    ("MERCH-009", "Apple Store", "electronics", "US", "Cupertino")
]

PRODUCTS = [
    ("PROD-101", "MacBook Pro M3 Max", "laptops", 2499.00),
    ("PROD-102", "Sony WH-1000XM5 Headphones", "audio", 399.00),
    ("PROD-103", "Samsung Galaxy S24 Ultra", "smartphones", 1199.00),
    ("PROD-104", "Nike Air Max 270", "apparel", 160.00),
    ("PROD-105", "Dyson V15 Cordless Vacuum", "home_kitchen", 749.00),
    ("PROD-106", "LG OLED 65-inch 4K TV", "electronics", 1799.00),
    ("PROD-107", "NVIDIA RTX 4090 GPU", "electronics", 1699.00)  # High surge product
]

DEVICE_OS = ["iOS", "Android", "macOS", "Windows", "Linux", "unknown"]
EVENT_TYPES = ["view", "view", "view", "search", "add_to_cart", "remove_from_cart", "checkout"]


def generate_financial_transaction(simulate_fraud: bool = False) -> dict:
    """Generates a realistic financial transaction event."""
    tx_id = f"TX-{uuid.uuid4().hex[:12].upper()}"
    ts = datetime.now(timezone.utc).isoformat()

    if simulate_fraud:
        # Generate an intentional fraud pattern (rapid high value or high risk)
        user_id = random.choice(FRAUD_TARGET_USERS)
        merchant = random.choice([m for m in MERCHANTS if m[2] in ("crypto", "wire_transfer", "gambling")])
        amount = round(random.uniform(2600.0, 9500.0), 2)
        is_intl = True
        country = merchant[3]
        city = merchant[4]
        dev_os = random.choice(["unknown", "Linux"])
    else:
        user_id = random.choice(USERS)
        merchant = random.choice(MERCHANTS)
        amount = round(random.uniform(5.0, 450.0), 2)
        is_intl = random.random() < 0.05
        country = "US" if not is_intl else "UK"
        city = "New York" if not is_intl else "London"
        dev_os = random.choice(["iOS", "Android", "macOS", "Windows"])

    return {
        "transaction_id": tx_id,
        "user_id": user_id,
        "card_number_masked": f"4111-XXXX-XXXX-{random.randint(1000, 9999)}",
        "amount": amount,
        "currency": "USD",
        "merchant_id": merchant[0],
        "merchant_name": merchant[1],
        "merchant_category": merchant[2],
        "location_country": country,
        "location_city": city,
        "ip_address": f"{random.randint(11, 199)}.{random.randint(10, 250)}.{random.randint(1, 250)}.{random.randint(1, 250)}",
        "device_id": f"DEV-{uuid.uuid4().hex[:8].upper()}",
        "device_os": dev_os,
        "timestamp": ts,
        "is_international": is_intl
    }


def generate_ecommerce_event(surge_product: bool = False) -> dict:
    """Generates a realistic e-commerce clickstream or cart action."""
    event_id = f"EVT-{uuid.uuid4().hex[:12].upper()}"
    ts = datetime.now(timezone.utc).isoformat()
    session_id = f"SES-{random.randint(100, 999)}"
    user_id = random.choice(USERS)

    if surge_product:
        # Simulate traffic surge on NVIDIA GPU / MacBook for dynamic pricing trigger
        prod = random.choice([p for p in PRODUCTS if p[0] in ("PROD-107", "PROD-101")])
        event_type = random.choice(["view", "view", "add_to_cart", "add_to_cart"])
    else:
        prod = random.choice(PRODUCTS)
        event_type = random.choice(EVENT_TYPES)

    quantity = 1 if event_type in ("add_to_cart", "checkout") else 0

    return {
        "event_id": event_id,
        "session_id": session_id,
        "user_id": user_id,
        "event_type": event_type,
        "product_id": prod[0],
        "product_name": prod[1],
        "category": prod[2],
        "price": prod[3],
        "quantity": quantity,
        "dwell_time_seconds": random.randint(3, 180),
        "referrer": random.choice(["google.com", "instagram", "tiktok", "direct", "email"]),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "ip_address": f"{random.randint(50, 150)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "timestamp": ts
    }


def main():
    parser = argparse.ArgumentParser(description="Mock Stream Generator for Finance and E-Commerce")
    parser.add_argument("--mode", choices=["stdout", "kafka"], default="stdout",
                        help="Output mode: 'stdout' prints events, 'kafka' publishes to Kafka topics")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--finance-topic", default="financial_transactions", help="Kafka topic for finance")
    parser.add_argument("--ecommerce-topic", default="ecommerce_events", help="Kafka topic for e-commerce")
    parser.add_argument("--interval", type=float, default=0.5, help="Interval in seconds between events")
    parser.add_argument("--max-events", type=int, default=100, help="Maximum number of events to produce (0 = infinite)")

    args = parser.parse_args()

    producer = None
    if args.mode == "kafka":
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logger.info(f"Connected to Kafka broker at {args.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Could not connect to Kafka: {e}. Falling back to 'stdout' mode.")
            args.mode = "stdout"

    count = 0
    logger.info(f"Starting event stream generation in [{args.mode.upper()}] mode...")

    try:
        while True:
            # Alternately generate financial transactions and e-commerce clickstream
            is_fraud = (random.random() < 0.20)  # 20% simulated fraud rate
            finance_event = generate_financial_transaction(simulate_fraud=is_fraud)

            is_surge = (random.random() < 0.40)  # 40% surge traffic for dynamic pricing
            ecom_event = generate_ecommerce_event(surge_product=is_surge)

            if args.mode == "kafka" and producer:
                producer.send(args.finance_topic, value=finance_event)
                producer.send(args.ecommerce_topic, value=ecom_event)
                logger.info(f"[Kafka Sent] Finance TX: {finance_event['transaction_id']} (${finance_event['amount']}) | Ecom EVT: {ecom_event['event_type']} ({ecom_event['product_id']})")
            else:
                print(f"[FINANCE STREAM] {json.dumps(finance_event)}")
                print(f"[ECOMMERCE STREAM] {json.dumps(ecom_event)}")

            count += 1
            if args.max_events and count >= args.max_events:
                logger.info(f"Reached max events limit ({args.max_events}). Stopping generator.")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("Generator interrupted by user. Shutting down.")
    finally:
        if producer:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
