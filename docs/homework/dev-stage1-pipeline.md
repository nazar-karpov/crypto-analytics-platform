# Домашнее задание — Разработчик, Занятие 2
## Этап 1: Пайплайн данных под задачу

## 1. Уточнение требований у PO

Источник: Задания 1–2, PO — Магомедов Арсен (`Осокин_продакт_1_занятие.docx`,
`Осокин_продакт_09.07_2_занятие.docx`). Ниже — трассировка его требований
(FR/NFR) на реализацию.

### Трассировка функциональных требований

| FR | Требование PO | Статус | Комментарий |
|---|---|---|---|
| FR-01 | Ingest котировок (топ-50, CoinGecko) | ✅ | Kafka producer, poll ~30с |
| FR-02 | Буферизация потока (`raw_crypto_prices`) | ✅ | без изменений |
| FR-03 | Сырой слой STG (Parquet, партиции по дате/часу) | ✅ | bronze остаётся plain Parquet |
| FR-04 | Очищенный слой DDS (SCD2 + fact) | ✅ | DDS — Apache Iceberg-таблицы (`dds.coins_dim`, `dds.prices_fact`): ACID-снапшоты, SCD2-измерение, append-only факт |
| FR-05 | Начальная загрузка (`initial_load`) | ✅ | без изменений |
| FR-06 | Витрина OHLCV | ✅ | `mart_hourly_ohlcv` |
| FR-07 | Витрина топ-муверов | ✅ | `mart_top_movers` |
| FR-08 | Витрина обзора рынка | ✅ | `mart_market_overview` |
| FR-09 | Витрина статистики монеты | ✅ | `mart_coin_stats` |
| FR-10 | Оркестрация Airflow | ✅ | без изменений |
| FR-11 | BI-дашборд Grafana | ✅ | без изменений |
| FR-12 | AI self-service (NL → SQL агент) | — | не входит в текущую реализацию |
| FR-13 | Идемпотентность повторного прогона | ✅ (усилено) | 3 последовательных запуска `process_stg_to_dds` без дублей факта; в DDS — за счёт Iceberg ACID, в DDM — `ReplacingMergeTree` |
| FR-14 | Наблюдаемость (Airflow/MinIO/ClickHouse UI) | ✅ | + MinIO console показывает Iceberg data/metadata в бакете `lakehouse` |

### Трассировка нефункциональных требований

| NFR | Требование PO | Статус | Комментарий |
|---|---|---|---|
| NFR-01 | Задержка API→STG, минуты | ✅ | без изменений |
| NFR-02 | Цикл обновления DDM ≤5–60 мин | ✅ | `stg_to_dds` каждые 5 мин |
| NFR-03 | Подъём одной командой, восстановление после рестарта | ✅ (проверено) | Iceberg-каталог отдельно протестирован на persistence — таблицы/данные/history снапшотов переживают `docker compose restart iceberg-rest` |
| NFR-04 | Секреты не в репозитории | ✅ | без изменений |
| NFR-05 | Доступ агента read-only | — | N/A, агент не входит в реализацию |
| NFR-06 | PII / 152-ФЗ | ✅ | без изменений |
| NFR-07 | Self-hosted OSS, 8–16 ГБ RAM | ⚠️ | Iceberg REST catalog — ещё один JVM-контейнер; полный стек (~15 сервисов) не прогонялся одновременно за один заход в этой сессии, тестировался изолированными подмножествами — см. Этап 3 |
| NFR-08 | Смена среды без смены модели данных | ✅ (усилено) | это ровно критерий Vendor Lock-in 5/5 из Battle-card PO (Задание 1) — Iceberg-таблицы читаются любым совместимым движком (Spark/Trino/Snowflake) при переезде в managed-облако без миграции данных |

### DoD (Definition of Done)

PO сформулировал DoD: *«compose up → данные в STG/DDS → 4 mart заполнены →
дашборд и демонстрация lakehouse-возможностей»*. В эту итерацию входят
снапшоты, time travel и schema evolution (`scripts/demo_lakehouse_features.py`)
и второй движок, читающий те же Iceberg-таблицы (`scripts/query_lakehouse_duckdb.py`).
AI-агент (FR-12) вне scope этой итерации.

## 2. Согласование с архитектором

Полные артефакты: [Этап 1 — архитектура](stage1-architecture.md),
[Этап 2 — согласование PO/Dev](stage2-po-dev-alignment.md).

Коротко, что уже зафиксировано архитектором и не пересматривается здесь:
- Табличный формат: **Apache Iceberg**, не Delta/Hudi (ADR-2)
- Каталог: **REST catalog** (`tabulario/iceberg-rest`), не JDBC/Postgres — решение изменилось уже на этапе передачи разработчику из-за конфликта SQLAlchemy с Airflow
- Serving-слой: ClickHouse остаётся отдельной копией данных для BI — Grafana не подключается к Iceberg напрямую (ADR-3)
- Ad-hoc движок: DuckDB, не Trino/Spark SQL (ADR-4)

## 3. Развёртывание нужных сервисов — что реально поднято и проверено

Весь docker-compose разом не поднимался (см. NFR-07) — разворачивались
изолированные подмножества под конкретные проверки.

**Сервисы:**
```bash
docker compose up -d minio minio-init iceberg-rest
```
`iceberg-rest`: healthcheck на `curl` не работает (в образе нет
`curl`/`wget`), заменён на TCP-probe: `bash -c "exec 3<>/dev/tcp/localhost/8181"`.
Healthy за ~10 сек.

**Изолированный smoke-test (вне Airflow):**
- `create_table_if_not_exists` → `dds.coins_dim`, `dds.prices_fact` созданы
- `append()` × 2 → 2 строки → 4 строки в `prices_fact`
- `table.history()` → ≥2 снапшота
- time travel: `scan(snapshot_id=...)` — старый снапшот 2 строки, новый 4 строки
- `update_schema().add_column(...)` — новая колонка видна, старые строки читаются без ошибок
- DuckDB (`iceberg_scan(metadata_location)`) — читает те же 4 строки напрямую, без ClickHouse

**Реальная функция DAG `process_stg_to_dds` на seeded STG-данных:**
- запуск 1: 2 STG-строки → 2 факта, 2 dim
- запуск 2: новый час + новая монета + переименование монеты → факт вырос до 5, SCD2 корректно закрыл старую версию `ethereum` и открыл новую
- запуск 3 (без новых STG-данных): факт остался 5 строк — дублей нет

**Persistence:** `docker compose restart iceberg-rest` → таблицы, 4 строки
факта и 2 снапшота истории пережили рестарт (каталог настроен на
persistent SQLite volume, не in-memory).

Все контейнеры и volume после тестов удалены (`docker compose down -v`) —
чистый старт для финального прогона перед защитой.

## Артефакт

Этот документ + трассировка FR/NFR выше.
