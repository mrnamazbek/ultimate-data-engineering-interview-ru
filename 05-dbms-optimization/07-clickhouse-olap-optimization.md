# Оптимизация ClickHouse (ClickHouse Performance Engineering)

ClickHouse — сверхбыстрая колоночная аналитическая СУБД для обработки больших объемов данных в реальном времени. В ClickHouse запросы выполняются со скоростью сотен миллионов строк в секунду благодаря векторизованному движку выполнения, SIMD-инструкциям процессора и семейству движков **MergeTree**.

---

## 1. Движок семейства MergeTree: ORDER BY и PRIMARY KEY

В ClickHouse ключевую роль играет порядок сортировки данных на диске.

```text
Строки данных:       [1 .. 8192] | [8193 .. 16384] | [16385 .. 24576]
Разреженный индекс:  Марка 0     | Марка 1         | Марка 2
                     (Каждые 8192 строки создается одна индексная засечка)
```

### Первичный ключ (PRIMARY KEY) в ClickHouse:
1. **Не гарантирует уникальность**: строки с абсолютно одинаковым ключом могут спокойно существовать в таблице.
2. **Является разреженным (Sparse Index)**: ClickHouse не индексирует каждую строку (как B-Tree). Индекс хранит только одно значение на каждые 8192 строки (`index_granularity = 8192`). 
3. Благодаря этому индекс по миллиардам строк занимает всего несколько мегабайт и целиком помещается в кэш процессора L3 или ОЗУ.

### Правило выбора порядка колонок в `ORDER BY`:
Колонки в ключе сортировки должны располагаться **от наименее селективных к наиболее селективным**:
```sql
CREATE TABLE analytics.events (
    tenant_id UInt32,
    event_type LowCardinality(String),
    event_time DateTime,
    user_id UInt64,
    payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)           -- Партиционирование по месяцам
PRIMARY KEY (tenant_id, event_type)         -- Разреженный индекс
ORDER BY (tenant_id, event_type, event_time, user_id); -- Физический порядок на диске
```
*Почему такой порядок*: Данные с одинаковым `tenant_id` и `event_type` группируются рядом, обеспечивая максимальное сжатие алгоритмами LZ4/ZSTD и мгновенное отсечение блоков при фильтрации в `WHERE`.

---

## 2. Специализированные движки MergeTree

- **MergeTree**: базовый движок для неизменяемых сырых логов (Append-only).
- **ReplacingMergeTree(ver)**: автоматически удаляет дубликаты строк с одинаковым `ORDER BY` во время фонового слияния кусков (Compaction). Для форсирования дедупликации в запросе используют модификатор `FINAL`:
  ```sql
  SELECT * FROM users_replacing FINAL WHERE id = 101;
  ```
- **SummingMergeTree(amount)**: автоматически суммирует числовые метрики для строк с одинаковым первичным ключом при слиянии кусков.
- **AggregatingMergeTree**: сохраняет промежуточные состояния агрегатов (например, промежуточные структуры HyperLogLog `AggregateFunction(uniq, UInt64)`) для построения молниеносных материализованных представлений (Materialized Views).

---

## 3. Пропускные индексы (Data Skipping Indexes)

Если запрос фильтрует по колонке, которая **не входит** в основной `ORDER BY`, ClickHouse по умолчанию сканирует таблицу целиком. Чтобы избежать полного сканирования, создают пропускные индексы:

```sql
ALTER TABLE analytics.events ADD INDEX idx_user_bloom user_id 
TYPE bloom_filter(0.01) GRANULARITY 1;
-- Фильтр Блума позволяет пропустить чтение блоков данных, где user_id гарантированно отсутствует
```

### Основные типы:
1. `minmax`: сохраняет минимальное и максимальное значение колонки на блок гранул.
2. `set(max_rows)`: сохраняет уникальные значения, если их число не превышает порог.
3. `bloom_filter(false_positive_rate)`: фильтр Блума для строковых полей высокой кардинальности (UUID, URL, токены).

---

## 4. Золотые правила вставки данных (INSERT)

ClickHouse оптимизирован под чтение и тяжелую пакетную вставку, но не терпит частых мелких записей.

### Главный антипаттерн ClickHouse:
Попытка слать единичные `INSERT` по 1 строке от бэкенда (например, 1000 вставок в секунду).
- Каждый `INSERT` создает физическую директорию на диске (новый кусок данных — **Part**).
- ClickHouse не успевает фоново объединять миллионы кусков, и сервер выбрасывает критическую ошибку:
  **`Too many parts in all data in table. Merges are processing significantly slower than inserts`**.

### Правильный подход:
1. Буферизовать данные на стороне приложения или брокера (Kafka) и отправлять батчами: **от 10 000 до 100 000 строк** за один запрос.
2. Использовать таблицы буферизации: `ENGINE = Buffer(...)` для накопления данных в ОЗУ перед сбросом в постоянную таблицу.

---

## 5. Оптимизация JOIN и Словари (Dictionaries)

Традиционный `JOIN` в ClickHouse распределяет правую таблицу целиком в оперативную память всех нод. Если правая таблица слишком большая, сервер падает с ошибкой нехватки памяти.

### Оптимизация через внешние словари (ClickHouse Dictionaries):
Для справочников измерений (пользователи, валюты, товары) создают In-Memory словари, которые периодически обновляются из MySQL/PostgreSQL:
```sql
-- Мгновенное обогащение лога без использования JOIN
SELECT 
    event_time,
    user_id,
    dictGet('user_dict', 'country', user_id) AS country_name,
    dictGet('user_dict', 'is_vip', user_id) AS is_vip
FROM analytics.events
WHERE event_time >= yesterday();
```
Вызов `dictGet` работает со скоростью чтения хэш-таблицы в оперативной памяти процессора ($O(1)$) и в 10–20 раз быстрее любого `JOIN`.

---

## 6. Тип данных `LowCardinality`

Для строковых колонок с ограниченным набором уникальных значений (до 10 000 уникальных вариантов: статус заказа, метод HTTP, код валюты, город):
```sql
-- Обычная строка:
status String

-- Оптимизированная колонка:
status LowCardinality(String)
```
ClickHouse автоматически заменяет строки на целочисленные индексы словаря. Это сокращает объем таблицы на диске в 3–5 раз и ускоряет фильтрацию и агрегацию в разы благодаря векторным инструкциям процессора (SIMD).

---

## 7. Официальные источники и документация вендора

Материалы основаны на официальной документации ClickHouse (версии 23.x / 24.x):
- [ClickHouse Documentation: MergeTree Family Engines](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree) — устройство разреженного индекса, гранулярность (`index_granularity = 8192`), механизм фоновых слияний (Parts & Merges).
- [ClickHouse Documentation: Primary Keys and Sorting Keys](https://clickhouse.com/docs/en/optimize/sparse-primary-indexes) — разница между `PRIMARY KEY` и `ORDER BY`, правила упорядочивания колонок от низкой кардинальности к высокой.
- [ClickHouse Documentation: Data Skipping Indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes) — пропускные индексы `minmax`, `set`, `bloom_filter` и параметры гранулярности.
- [ClickHouse Documentation: Dictionaries](https://clickhouse.com/docs/en/sql-reference/dictionaries) — настройка внешних In-Memory словарей (`dictGet`) для высокопроизводительного обогащения данных без `JOIN`.
- [ClickHouse Documentation: Bulk Inserts & Async Inserts](https://clickhouse.com/docs/en/optimize/bulk-inserts) — рекомендации по размерам батчей и настройка `async_insert = 1` для предотвращения ошибки `Too many parts`.

