import json
import math
import pandas as pd
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, DATASETS


def clean_value(v):
    """Membersihkan nilai NaN / Inf agar valid saat diserialisasi ke JSON."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    batch_flush = 5_000

    print("=" * 65)
    print("  Olist Kafka Producer — Memulai Proses Ingestion 9 Dataset CSV")
    print("=" * 65)

    try:
        for topic, filepath in DATASETS.items():
            print(f"\n▶  {filepath}  →  topik '{topic}'")
            try:
                df = pd.read_csv(filepath, low_memory=False)
            except FileNotFoundError:
                print(f"   ⚠  File tidak ditemukan: {filepath}. Dilewati.")
                continue

            total = len(df)
            for idx, row in enumerate(df.to_dict(orient="records"), start=1):
                record = {k: clean_value(v) for k, v in row.items()}
                producer.send(topic, record)

                if idx % batch_flush == 0:
                    producer.flush()
                    print(f"   {idx:>10,} / {total:,} baris terkirim...")

            producer.flush()
            print(f"   ✓  Selesai — {total:,} baris dikirim ke '{topic}'")

        print("\n" + "=" * 65)
        print("  Ingestion selesai untuk seluruh dataset Olist.")
        print("=" * 65)
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
