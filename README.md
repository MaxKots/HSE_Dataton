# Дататон ВШЭ
## Оглавление:
1. [Подготовка компонентов](#подготовка-компонентов)
3. [Как войти в сервисы](#Как-войти-в-сервисы)
4. [Проверка работоспособности](#Проверка-работоспособности)

## Подготовка компонентов
### Структура проекта(чуть устарел, пока делал, чуть позже поменяю на актуальный):

```text
|--- docker-compose.yml
|--- .env
|--- dags/
│   --- dv_pipeline.py
|--- spark/
│   --- jobs/
│       --- load_to_vault.py
|--- data/
│   --- source/
|       --- CD_banner.csv
|       --- CD_campaign.csv
|       --- CD_user.csv
|       --- Fct_actions.csv
|       --- Fct_banners_show.csv
|       --- Installs.csv
|--- scripts/
│   --- producer.py
│   --- consumer.py
|--- configs/
|   --- spark-defaults.conf
```

## Заполнение файлов
<details>
<summary><b>Башник для создания структуры файлов</b></summary>

```Bash
#!/usr/bin/env bash

set -e

PROJECT="project"

echo "creating project structure in ./$PROJECT"

mkdir -p "$PROJECT"/{dags,spark/jobs,data/source,scripts}

touch "$PROJECT"/dags/.gitkeep #чтоб проблем с гитом не было
touch "$PROJECT"/spark/jobs/.gitkeep #чтоб проблем с гитом не было
touch "$PROJECT"/docker-compose.yml
touch "$PROJECT"/.env
touch "$PROJECT"/scripts/producer.py
touch "$PROJECT"/scripts/consumer.py

echo "Заебумба, структура создана"
find "$PROJECT"
```
</details>

Закинь csv из архива коллег по пути:
.../project/data/source

Во избежание явного отображения паролей в connections без кодированя паролей и прочих чувствительных данных в Airflow, создал рандомный fernet_key:

```text
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

<details>
<summary><b>Башник для заполнения .env файла</b></summary>

```Bash
#!/usr/bin/env bash

set -e

PROJECT="project"

echo "Заполняю .env"

cat > "$PROJECT/.env" << 'EOF'
AIRFLOW_UID=50000

POSTGRES_DWH_USER=***USER***
POSTGRES_DWH_PASSWORD=***PASSWORD***
POSTGRES_DWH_DB=dataton

POSTGRES_AF_USER=***USER***
POSTGRES_AF_PASSWORD=***PASSWORD***
POSTGRES_AF_DB=airflow

MINIO_USER=***USER***
MINIO_PASSWORD=***PASSWORD***

FERNET_KEY=***KEY***
EOF

echo " Файлик .env заполнен"
```
</details>

<details>
<summary><b>Башник для заполнения продюсера кафки</b></summary>

```Bash
#!/usr/bin/env bash

cat > "$PROJECT/scripts/producer.py" << 'PYEOF'
"""
Эта шляпа будет читать все CSV из ./data/, каждую строку отправляет
в кафку как json с полем имени файла без расширения(потом допилим)
"""

import json
import os
import csv
import time
from pathlib import Path
from confluent_kafka import Producer

BROKER = os.environ["BROKER"]
TOPIC = os.environ["TOPIC"]
DATA_DIR = Path("data")

conf = {
    "bootstrap.servers": BROKER,
    "queue.buffering.max.messages": 100000,
    "batch.num.messages": 500,
    "linger.ms": 50,
}
producer = Producer(conf)

errors = 0


def on_delivery(err, msg):
    global errors
    if err:
        errors += 1
        print(f" {err}")


def send_file(path: Path):
    name = path.stem
    size_kb = path.stat().st_size / 1024
    print(f"\n→ {name} ({size_kb:.0f} KB)")

    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["_source"] = name
            payload = json.dumps(row, ensure_ascii=False)

            producer.produce(
                TOPIC,
                key=name,
                value=payload.encode("utf-8"),
                callback=on_delivery,
            )
            count += 1

            # периодически дергаем poll, чтобы не переполнить очередь
            if count % 1000 == 0:
                producer.poll(0)

    producer.flush(timeout=30)
    print(f" {count} строк")
    return count


def main():
    files = sorted(DATA_DIR.glob("*.csv"))
    if not files:
        print(f"нет csv-файлов в {DATA_DIR.resolve()}")
        return

    print(f"файлов: {len(files)}, broker: {BROKER}, topic: {TOPIC}")

    total = 0
    t0 = time.time()
    for f in files:
        total += send_file(f)

    elapsed = time.time() - t0
    print(f"\nитого: {total} строк за {elapsed:.1f}с, ошибок: {errors}")


if __name__ == "__main__":
    main()
PYEOF

echo "Файлик scripts/producer.py заполнен"
```
</details>

<details> <summary><b>Башник для заполнения консьюмера кафки</b></summary>

```Bash
#!/usr/bin/env bash

cat > "$PROJECT/scripts/consumer.py" << 'PYEOF'
"""
Читает сообщения из Kafka, группирует по названию источника,
по достижении лимита или таймаута сбрасывает
пачку в minio как parquet
"""

import json
import os
import io
import time
from datetime import datetime, timezone
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

BATCH = int(os.environ.get("BATCH_SIZE", "1000"))
FLUSH_SEC = int(os.environ.get("FLUSH_SEC", "10"))

store = Minio(
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
    "session.timeout.ms": 30000,
})
consumer.subscribe([TOPIC])

buf = defaultdict(list)
files_written = 0


def flush():
    """Собирает DataFrame - parquet - minio для каждого источника"""
    global files_written

    if not any(buf.values()):
        return

    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    for source, rows in buf.items():
        if not rows:
            continue

        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, preserve_index=False)

        out = io.BytesIO()
        pq.write_table(table, out, compression="snappy")
        size = out.tell()
        out.seek(0)

        key = f"{source}/{now}_{len(rows)}.parquet"
        store.put_object(
            BUCKET, key, out, size,
            content_type="application/octet-stream",
        )
        files_written += 1
        print(f"  ✔ s3://{BUCKET}/{key}  ({len(rows)} rows, {size/1024:.1f} KB)")

    buf.clear()


def main():
    print(f"consumer started: topic={TOPIC} group={GROUP} bucket={BUCKET}")
    print(f"  batch={BATCH}, flush every {FLUSH_SEC}s")

    last_flush = time.time()
    total_msgs = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if time.time() - last_flush > FLUSH_SEC and buf:
                    flush()
                    last_flush = time.time()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            row = json.loads(msg.value().decode("utf-8"))
            source = row.pop("_source", "unknown")
            buf[source].append(row)
            total_msgs += 1

            count = sum(len(v) for v in buf.values())
            if count >= BATCH:
                flush()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nостановка...")
    finally:
        flush()
        consumer.close()
        print(f"consumer stopped: {total_msgs} msgs, {files_written} files")


if __name__ == "__main__":
    main()
PYEOF

echo " Файлик scripts/consumer.py заполнен"
```
</details>

Так как мой docker не понимает docker-inline, созхдал отдельные docker-файлы для консьюмера и продюсера:

<details> <summary><b>Башник для заполнения docker-файлов консьюмера и продюсера</b></summary>

```Bash
#!/usr/bin/env bash

# 1 продюсер
cat > Dockerfile.producer <<EOF
FROM python:3.12.3
RUN pip install --no-cache-dir confluent-kafka
WORKDIR /app
COPY scripts/producer.py .
COPY data/source/ ./data/
CMD ["python", "producer.py"]
EOF

# 2 консьюмер
cat > Dockerfile.consumer <<EOF
FROM python:3.12.3
RUN pip install --no-cache-dir confluent-kafka minio pyarrow pandas
WORKDIR /app
COPY scripts/consumer.py .
CMD ["python", "consumer.py"]
EOF
```
</details>

### Как запустить:

```text

docker compose up -d

посмотри там происходит
docker compose logs -f producer consumer

останови
docker compose down -v
```

## Как войти в сервисы:

```text

Airflow	  localhost:8080
Kafka UI	localhost:8090
MinIO	    localhost:9001
Spark UI	localhost:8081
Postgres	localhost:5433
```

## Проверка работоспособности

