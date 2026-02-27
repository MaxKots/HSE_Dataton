version: "3.8"

# ──────────────────────────────────────────────
#  Общий блок для Airflow-сервисов
# ──────────────────────────────────────────────
x-airflow: &airflow-base
  image: apache/airflow:2.9.3-python3.11
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__FERNET_KEY: ${FERNET_KEY}
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: >-
      postgresql+psycopg2://${POSTGRES_AF_USER}:${POSTGRES_AF_PASSWORD}@af-postgres:5432/${POSTGRES_AF_DB}
    MINIO_ENDPOINT: http://minio:9000
    MINIO_ACCESS_KEY: ${MINIO_USER}
    MINIO_SECRET_KEY: ${MINIO_PASSWORD}
    KAFKA_BOOTSTRAP: kafka:29092
    DWH_CONN: postgresql://${POSTGRES_DWH_USER}:${POSTGRES_DWH_PASSWORD}@dwh-postgres:5432/${POSTGRES_DWH_DB}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./spark/jobs:/opt/airflow/spark-jobs
    - ./data/source:/opt/airflow/data
    - af-logs:/opt/airflow/logs
  depends_on:
    af-postgres:
      condition: service_healthy
  networks:
    - dv

services:

  # ════════════════════════════════════════════
  #  PostgreSQL для метаданных Airflow
  # ════════════════════════════════════════════
  af-postgres:
    image: postgres:16-alpine
    container_name: af-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_AF_USER}
      POSTGRES_PASSWORD: ${POSTGRES_AF_PASSWORD}
      POSTGRES_DB: ${POSTGRES_AF_DB}
    volumes:
      - af-pg:/var/lib/postgresql/data
    healthcheck:
      test: pg_isready -U ${POSTGRES_AF_USER}
      interval: 4s
      retries: 12
    ports:
      - "5432:5432"
    networks:
      - dv

  # ════════════════════════════════════════════
  #  PostgreSQL — целевое хранилище Data Vault
  # ════════════════════════════════════════════
  dwh-postgres:
    image: postgres:16-alpine
    container_name: dwh-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_DWH_USER}
      POSTGRES_PASSWORD: ${POSTGRES_DWH_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DWH_DB}
    volumes:
      - dwh-pg:/var/lib/postgresql/data
    healthcheck:
      test: pg_isready -U ${POSTGRES_DWH_USER}
      interval: 4s
      retries: 12
    ports:
      - "5433:5432"
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Airflow — первый запуск
  # ════════════════════════════════════════════
  airflow-boot:
    <<: *airflow-base
    container_name: airflow-boot
    restart: "no"
    entrypoint: bash -c
    command:
      - |
        airflow db migrate
        airflow users create \
          -u admin -p admin \
          -f Admin -l User \
          -r Admin -e admin@local.dev || true

  # ════════════════════════════════════════════
  #  Airflow — веб-интерфейс
  # ════════════════════════════════════════════
  airflow-web:
    <<: *airflow-base
    container_name: airflow-web
    command: webserver
    ports:
      - "8080:8080"
    restart: unless-stopped
    depends_on:
      airflow-boot:
        condition: service_completed_successfully

  # ════════════════════════════════════════════
  #  Airflow — планировщик
  # ════════════════════════════════════════════
  airflow-sched:
    <<: *airflow-base
    container_name: airflow-sched
    command: scheduler
    restart: unless-stopped
    depends_on:
      airflow-boot:
        condition: service_completed_successfully

  # ════════════════════════════════════════════
  #  ZooKeeper
  # ════════════════════════════════════════════
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zk-data:/var/lib/zookeeper/data
      - zk-txn:/var/lib/zookeeper/log
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Kafka — один брокер
  # ════════════════════════════════════════════
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: INSIDE://kafka:29092,OUTSIDE://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INSIDE
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: kafka-broker-api-versions --bootstrap-server localhost:9092
      interval: 8s
      retries: 15
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Kafka — создание топиков
  # ════════════════════════════════════════════
  kafka-setup:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka-setup
    restart: "no"
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: bash -c
    command:
      - |
        for topic in raw-ingest staging-events vault-load; do
          kafka-topics --bootstrap-server kafka:29092 \
            --create --if-not-exists \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1
          echo "  ✔ $topic"
        done
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Kafka — продюсер
  # ════════════════════════════════════════════
  producer:
    build:
      context: .
      dockerfile_inline: |
        FROM python:3.11-slim
        RUN pip install --no-cache-dir confluent-kafka
        WORKDIR /app
        COPY scripts/producer.py .
        COPY data/source/ ./data/
        CMD ["python", "producer.py"]
    container_name: producer
    environment:
      BROKER: kafka:29092
      TOPIC: raw-ingest
    depends_on:
      kafka-setup:
        condition: service_completed_successfully
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Kafka — консьюмер
  # ════════════════════════════════════════════
  consumer:
    build:
      context: .
      dockerfile_inline: |
        FROM python:3.11-slim
        RUN pip install --no-cache-dir confluent-kafka minio pyarrow pandas
        WORKDIR /app
        COPY scripts/consumer.py .
        CMD ["python", "consumer.py"]
    container_name: consumer
    environment:
      BROKER: kafka:29092
      TOPIC: raw-ingest
      GROUP: dv-consumers
      MINIO_HOST: minio:9000
      MINIO_AK: ${MINIO_USER}
      MINIO_SK: ${MINIO_PASSWORD}
      BUCKET: raw-zone
    depends_on:
      kafka-setup:
        condition: service_completed_successfully
      minio-setup:
        condition: service_completed_successfully
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Kafka UI
  # ════════════════════════════════════════════
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: datavault-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - dv

  # ════════════════════════════════════════════
  #  MinIO
  # ════════════════════════════════════════════
  minio:
    image: minio/minio:RELEASE.2024-06-13T22-53-53Z
    container_name: minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_PASSWORD}
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: mc ready local
      interval: 6s
      retries: 10
    networks:
      - dv

  # ════════════════════════════════════════════
  #  MinIO — создание бакетов
  # ════════════════════════════════════════════
  minio-setup:
    image: minio/mc:latest
    container_name: minio-setup
    restart: "no"
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: bash -c
    command:
      - |
        mc alias set s3 http://minio:9000 ${MINIO_USER} ${MINIO_PASSWORD}
        for b in raw-zone staging-zone curated-zone; do
          mc mb --ignore-existing s3/$b
          echo "  ✔ $b"
        done
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Spark — мастер
  # ════════════════════════════════════════════
  spark-master:
    image: bitnami/spark:3.5.1
    container_name: spark-master
    environment:
      SPARK_MODE: master
      AWS_ACCESS_KEY_ID: ${MINIO_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_PASSWORD}
    ports:
      - "8081:8080"
      - "7077:7077"
    volumes:
      - ./spark/jobs:/opt/spark-jobs
      - ./data/source:/opt/spark-data
    networks:
      - dv

  # ════════════════════════════════════════════
  #  Spark — воркер
  # ════════════════════════════════════════════
  spark-worker:
    image: bitnami/spark:3.5.1
    container_name: spark-worker
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2g
      SPARK_WORKER_CORES: 2
      AWS_ACCESS_KEY_ID: ${MINIO_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_PASSWORD}
    depends_on:
      - spark-master
    volumes:
      - ./spark/jobs:/opt/spark-jobs
      - ./data/source:/opt/spark-data
    networks:
      - dv

volumes:
  af-pg:
  dwh-pg:
  zk-data:
  zk-txn:
  kafka-data:
  minio-data:
  af-logs:

networks:
  dv:
    driver: bridge
