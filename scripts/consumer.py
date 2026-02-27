"""
Слушает топик Kafka, накапливает пачку сообщений и сбрасывает
в MinIO как parquet-файл (один файл на каждый _source).
"""

import json
import os
import io
from datetime import datetime
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException
from minio import Minio

BROKER = os.environ["BROKER"]
TOPIC = os.environ["TOPIC"]
GROUP = os.environ["GROUP"]
BUCKET = os.environ["BUCKET"]
BATCH_SIZE = 1000
FLUSH_INTERVAL = 10  # секунд

minio = Minio(
    os.environ["MINIO_HOST"],
    access_key=os.environ["MINIO_AK"],
    secret_key=os.environ["MINIO_SK"],
    secure=False,
)

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
consumer.subscribe([TOPIC])

buffer = defaultdict(list)


def flush_buffer():
    """Превращает накопленные записи в parquet и кладёт в MinIO."""
    if not any(buffer.values()):
        return

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    for source, rows in buffer.items():
        if not rows:
            continue

        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        size = buf.getbuffer().nbytes

        key = f"{source}/{ts}_{len(rows)}.parquet"
        minio.put_object(BUCKET, key, buf, size, content_type="application/octet-stream")
        print(f"  ✔ {key}  ({len(rows)} rows, {size / 1024:.1f} KB)")

    buffer.clear()


def main():
    import time

    print(f"consumer started — topic={TOPIC}, group={GROUP}")
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if time.time() - last_flush > FLUSH_INTERVAL and buffer:
                    flush_buffer()
                    last_flush = time.time()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            row = json.loads(msg.value().decode("utf-8"))
            source = row.pop("_source", "unknown")
            buffer[source].append(row)

            total = sum(len(v) for v in buffer.values())
            if total >= BATCH_SIZE:
                flush_buffer()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        flush_buffer()
        consumer.close()
        print("consumer stopped")


if __name__ == "__main__":
    main()
