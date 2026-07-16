# Real-time Crypto Analytics Platform

Платформа аналитики криптовалют с потоковой обработкой данных
и 3-слойной lakehouse-архитектурой (Apache Iceberg).

---

## Архитектура

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA FLOW                                 │
│                                                                  │
│  CoinGecko API (каждые 30 сек, топ-50 монет)                    │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────┐    Kafka topic:     ┌─────────────────────────┐   │
│  │  Kafka   │    raw_crypto_      │  MinIO (S3 хранилище)   │   │
│  │ Producer │    prices           │                         │   │
│  └────┬─────┘         │           │  stg/prices/            │   │
│       │               │           │    └─ year/month/day/   │   │
│       └───────────────┤           │       (raw Parquet)     │   │
│                       │           │                         │   │
│                       ▼           │  lakehouse/ (Iceberg)   │   │
│              ┌────────────────┐   │  dds.coins_dim (SCD2)   │   │
│              │ Python Consumer│   │  dds.prices_fact        │   │
│              │ (Kafka → STG)  │──▶│  (append-only, ACID)    │   │
│              └────────────────┘   └───────────┬─────────────┘   │
│                                               │                  │
│                              (catalog: iceberg-rest, REST API)   │
│                                               │                  │
│                                    ┌──────────▼──────────┐      │
│                                    │   Airflow (pandas +  │      │
│                                    │      PyIceberg)      │      │
│                                    │  ┌───────────────┐  │      │
│                                    │  │  stg_to_dds   │  │      │
│                                    │  └───────┬───────┘  │      │
│                                    │  ┌───────▼───────┐  │      │
│                                    │  │  dds_to_ddm   │  │      │
│                                    │  └───────┬───────┘  │      │
│                                    └──────────┼──────────┘      │
│                                               ▼                  │
│                                    ┌────────────────────┐       │
│                                    │    ClickHouse       │       │
│                                    │  mart_hourly_ohlcv  │       │
│                                    │  mart_top_movers    │       │
│                                    │  mart_market_over.. │       │
│                                    │  mart_coin_stats    │       │
│                                    └─────────┬──────────┘       │
│                                              │                   │
│                                              ▼                   │
│                                        ┌──────────┐             │
│                                        │ Grafana  │             │
│                                        │Dashboard │             │
│                                        └──────────┘             │
└──────────────────────────────────────────────────────────────────┘
```

### Слои данных (Lakehouse)

| Слой | Хранилище | Назначение |
|------|-----------|------------|
| **STG** (bronze) | `s3://stg/prices/` (raw Parquet) | Сырые данные из Kafka, partitioned по дате/часу |
| **DDS** (silver) | **Apache Iceberg**-таблицы `dds.coins_dim` / `dds.prices_fact` в `s3://lakehouse/`, каталог метаданных — Iceberg REST catalog (`iceberg-rest`) | Очищенные данные с ACID-гарантиями: SCD2-измерение монет (атомарный overwrite снапшота) + append-only таблица фактов (без перезаписи всей истории) |
| **DDM** (gold) | ClickHouse `crypto.*` | Агрегированные витрины для аналитики / serving-слой для Grafana |

STG сознательно остался обычным Parquet — там нет upsert/dedup-логики,
поэтому табличный формат ему не нужен. Табличный формат даёт пользу
именно в silver-слое, где происходят SCD2 и дедупликация.

### Витрины данных (DDM)

| Витрина | Описание |
|---------|----------|
| `mart_hourly_ohlcv` | OHLCV (Open/High/Low/Close/Volume) по каждой монете за час |
| `mart_top_movers` | Топ-10 растущих и падающих за 24ч |
| `mart_market_overview` | Общая капитализация, доминация BTC/ETH, объём торгов |
| `mart_coin_stats` | Средняя цена 7д/30д, волатильность, макс. просадка |

---

## Технологии

| Компонент | Технология |
|-----------|------------|
| Очередь сообщений | Apache Kafka |
| Объектное хранилище | MinIO (S3-совместимое) |
| **Табличный формат (lakehouse)** | **Apache Iceberg** (клиент — PyIceberg, без JVM/Spark в Airflow) |
| **Каталог метаданных Iceberg** | **Iceberg REST catalog** (`tabulario/iceberg-rest`) |
| Оркестрация | Apache Airflow |
| Обработка данных | Python + pandas |
| OLAP-хранилище / serving layer | ClickHouse |
| Второй движок поверх lakehouse (демо) | DuckDB (`iceberg_scan`) |
| Визуализация | Grafana |
| Контейнеризация | Docker Compose |

---

## Требования

- **Docker** 24+
- **Docker Compose** v2 (входит в Docker Desktop)
- **RAM**: 8 ГБ минимум (16 ГБ рекомендуется)
- **Диск**: ~5 ГБ для Docker-образов
- **Интернет** для CoinGecko API и скачивания образов

---

## Быстрый старт

### Linux / macOS
```bash
git clone <repo-url>
cd bigdata_hw
cp .env.example .env
docker compose up -d
```

### Windows (PowerShell)
```powershell
git clone <repo-url>
cd bigdata_hw
Copy-Item .env.example .env
docker compose up -d
```

Первый запуск займёт **5-10 минут** (скачиваются образы).

```bash
# Проверить что всё работает
docker compose ps

# Смотреть логи продюсера
docker compose logs -f kafka-producer
```

---

## Сервисы

| Сервис | URL | Логин |
|--------|-----|-------|
| **Kafka UI** | http://localhost:8080 | — |
| **MinIO** | http://localhost:9001 | admin / password123 |
| **Airflow** | http://localhost:8082 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **ClickHouse** | http://localhost:8123/play | — |

---

## Пайплайн данных

### Шаг 1 — Потоковый сбор (непрерывно)
`kafka-producer` опрашивает CoinGecko каждые 30 секунд и отправляет
данные о 50 монетах в Kafka. Python-consumer читает из Kafka и пишет
Parquet-файлы в MinIO `s3://stg/prices/`, партиционированные по
`year/month/day/hour`.

### Шаг 2 — STG → DDS (каждые 5 минут, Airflow)
DAG `stg_to_dds`:
- Читает свежие STG-партиции
- Дедуплицирует записи
- Применяет **SCD2** для измерения `dds.coins_dim` — пишет через
  `table.overwrite()` (Iceberg): атомарный снапшот, а не risk of torn write
- Дописывает новые записи в `dds.prices_fact` через `table.append()`
  (Iceberg) — без перезаписи всей истории фактов на каждом прогоне,
  дедуп-скан обрезается по времени благодаря partition-у `month(event_ts)`

### Шаг 3 — DDS → DDM (каждые 5 минут, Airflow)
DAG `dds_to_ddm`:
- Читает Iceberg-таблицы DDS через PyIceberg (`table.scan().to_pandas()`)
- Вычисляет 4 витрины
- Пишет в ClickHouse (ReplacingMergeTree)

### Историческая загрузка (опционально)
DAG `initial_load` загружает 7 дней истории с CoinGecko:
1. Открой http://localhost:8082
2. Найди DAG `initial_load`
3. Нажми ▶ (Trigger DAG)

---

## Lakehouse-возможности

Silver-слой (DDS) — это не просто Parquet-файлы, а настоящие **Apache
Iceberg**-таблицы: у них есть ACID-снапшоты, история версий, time
travel и schema evolution. Два демо-скрипта показывают это наглядно
(запусти `stg_to_dds` в Airflow хотя бы 2 раза с интервалом 5 минут,
чтобы накопилось несколько снапшотов):

```bash
# История снапшотов, time travel (сравнение строк "сейчас" и "раньше"),
# добавление колонки в схему без переписывания старых файлов
docker compose exec airflow-scheduler python /opt/airflow/scripts/demo_lakehouse_features.py

# Второй движок (DuckDB) читает те же Iceberg-таблицы напрямую по
# metadata-файлу — без pandas и без ClickHouse
docker compose exec airflow-scheduler python /opt/airflow/scripts/query_lakehouse_duckdb.py
```

Структуру Iceberg-таблиц (data/ и metadata/ файлы) можно увидеть в
MinIO console (http://localhost:9001) в бакете `lakehouse`.

---

## Мониторинг

```bash
# Статус всех сервисов
docker compose ps

# Логи конкретного сервиса
docker compose logs -f kafka-producer
docker compose logs -f spark-streaming
docker compose logs -f airflow-scheduler
docker compose logs -f iceberg-rest

# Запустить DAG вручную
docker compose exec airflow-scheduler airflow dags trigger stg_to_dds

# Запрос к ClickHouse
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM crypto.mart_hourly_ohlcv"
```

---

## Частые проблемы

| Проблема | Причина | Решение |
|----------|---------|---------|
| Grafana пустая | Витрины ещё не заполнены | Запусти `dds_to_ddm` вручную в Airflow |
| Airflow показывает ошибку ClickHouse | ClickHouse не готов | Подожди 1-2 минуты, запусти таску повторно |
| Airflow: ошибка подключения к каталогу | `iceberg-rest` ещё не готов | Подожди готовности `iceberg-rest`, запусти таску повторно |
| Vmmem жрёт всю память (Windows) | WSL2 без лимита | Создай `~/.wslconfig` с `memory=8GB` |
| Порт занят | Другой сервис на этом порту | `docker compose down`, освободи порт |

---

## Остановка и очистка

```bash
# Остановить всё
docker compose down

# Остановить и удалить все данные
docker compose down -v
```

---

## Проектные решения

1. **pandas вместо PySpark** — упрощает архитектуру, убирает 3 тяжёлых
   контейнера (spark-master, spark-worker, spark-streaming), экономит ~3 ГБ RAM.

2. **Python Kafka consumer вместо Spark Streaming** — простой скрипт
   читает из Kafka и пишет Parquet в MinIO. Надёжнее и легче дебажить.

3. **ReplacingMergeTree в ClickHouse** — позволяет идемпотентные запуски
   DAG; повторный запуск перезаписывает данные, а не дублирует.

4. **CoinGecko free API** — без API-ключа, лимит ~30 запросов/мин.
   Продюсер автоматически ждёт при получении 429.

5. **Apache Iceberg (через PyIceberg) вместо Delta Lake / Hudi** для DDS-слоя —
   Iceberg полноценно работает без Spark из чистого Python, что совпадает
   с уже принятым решением избавиться от JVM-стека. Delta Lake без Spark
   работает хуже (через менее зрелый `delta-rs`), Hudi тяжелее и хуже
   документирован для Python-only сценария.

6. **Iceberg REST catalog (`tabulario/iceberg-rest`), а не PyIceberg SQL-каталог
   на Postgres** — PyIceberg-экстра `sql-postgres`/`sql-sqlite` тянет
   SQLAlchemy 2.x, а Airflow 2.8 (через Flask-AppBuilder) жёстко требует
   SQLAlchemy `<2.0`; апгрейд ломает Airflow webserver. REST-каталог общается
   с Airflow только по HTTP, без SQLAlchemy — конфликта нет. Это же
   стандартная связка из официального Iceberg quickstart.
