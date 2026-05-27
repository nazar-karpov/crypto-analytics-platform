# Real-time Crypto Analytics Platform

Платформа аналитики криптовалют с потоковой обработкой данных,
3-слойной архитектурой lakehouse и AI-агентом для ответов на вопросы.

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
│                       ▼           │  dds/coins_dim/         │   │
│              ┌────────────────┐   │    └─ SCD2 dimension    │   │
│              │ Python Consumer│   │  dds/prices_fact/       │   │
│              │ (Kafka → STG)  │──▶│    └─ fact table        │   │
│              └────────────────┘   └───────────┬─────────────┘   │
│                                               │                  │
│                                    ┌──────────▼──────────┐      │
│                                    │   Airflow (pandas)   │      │
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
│                              ┌───────────────┴──────────┐       │
│                              ▼                           ▼       │
│                        ┌──────────┐             ┌────────────┐  │
│                        │ Grafana  │             │  AI Agent  │  │
│                        │Dashboard │             │  (FastAPI) │  │
│                        └──────────┘             └────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Слои данных (Lakehouse)

| Слой | Хранилище | Назначение |
|------|-----------|------------|
| **STG** | `s3://stg/prices/` | Сырые данные из Kafka, partitioned по дате/часу |
| **DDS** | `s3://dds/` | Очищенные данные; SCD2 измерение монет + таблица фактов |
| **DDM** | ClickHouse `crypto.*` | Агрегированные витрины для аналитики |

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
| Оркестрация | Apache Airflow |
| Обработка данных | Python + pandas |
| OLAP-хранилище | ClickHouse |
| Визуализация | Grafana |
| AI-агент | FastAPI + OpenRouter (GPT-4o-mini) |
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
# Отредактируй .env — добавь OPENROUTER_API_KEY
docker compose up -d
```

### Windows (PowerShell)
```powershell
git clone <repo-url>
cd bigdata_hw
Copy-Item .env.example .env
# Отредактируй .env — добавь OPENROUTER_API_KEY
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
| **AI Agent** | http://localhost:8000 | — |
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
- Применяет **SCD2** для измерения `coins_dim`
- Дописывает записи в `prices_fact`

### Шаг 3 — DDS → DDM (каждые 5 минут, Airflow)
DAG `dds_to_ddm`:
- Читает DDS из MinIO через pandas
- Вычисляет 4 витрины
- Пишет в ClickHouse (ReplacingMergeTree)

### Историческая загрузка (опционально)
DAG `initial_load` загружает 7 дней истории с CoinGecko:
1. Открой http://localhost:8082
2. Найди DAG `initial_load`
3. Нажми ▶ (Trigger DAG)

---

## AI Agent — Примеры вопросов

Открой http://localhost:8000 и спроси:

- *"Какая монета выросла больше всех за 24 часа?"*
- *"Какая сейчас доминация Bitcoin на рынке?"*
- *"Покажи топ-5 монет по объёму торгов"*
- *"Какая была максимальная цена Bitcoin сегодня?"*
- *"Сравни Bitcoin и Ethereum за последние 7 дней"*
- *"Какая общая капитализация крипторынка?"*
- *"Какие монеты больше всего упали сегодня?"*

Агент использует tool calling — генерирует SQL-запрос к ClickHouse,
получает данные и формирует ответ на естественном языке.

---

## Мониторинг

```bash
# Статус всех сервисов
docker compose ps

# Логи конкретного сервиса
docker compose logs -f kafka-producer
docker compose logs -f spark-streaming
docker compose logs -f airflow-scheduler
docker compose logs -f ai-agent

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
| AI Agent: "Internal Server Error" | Нет API ключа OpenRouter | Добавь `OPENROUTER_API_KEY` в `.env` |
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

5. **OpenRouter + GPT-4o-mini** — надёжный tool calling для SQL-запросов.
   Можно заменить на любую модель, изменив `OPENROUTER_MODEL` в `.env`.
