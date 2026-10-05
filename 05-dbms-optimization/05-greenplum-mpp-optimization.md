# Оптимизация Greenplum Database (MPP PostgreSQL)

Greenplum Database — аналитическая реляционная СУБД с массово-параллельной архитектурой (MPP — Massively Parallel Processing), построенная на базе ядра PostgreSQL. Она используется в качестве корпоративного DWH для петабайтных объемов. Оптимизация Greenplum принципиально отличается от классического PostgreSQL: ключевой задачей инженера становится устранение межсегментных сетевых перемещений (**Motion**) и ликвидация перекосов данных (**Data Skew**).

---

## 1. Архитектура Greenplum: Master и Сегменты

```text
                        Клиент / BI-инструмент
                                 |
                     +-----------v-----------+
                     |      Master Host      |
                     | (Парсинг, оптимизатор)|
                     +-----------+-----------+
                                 | Сеть Interconnect
            +--------------------+--------------------+
            |                    |                    |
    +-------v-------+    +-------v-------+    +-------v-------+
    |   Segment 1   |    |   Segment 2   |    |   Segment N   |
    | (Свой CPU/RAM/|    | (Свой CPU/RAM/|    | (Свой CPU/RAM/|
    |  диск NVMe)   |    |  диск NVMe)   |    |  диск NVMe)   |
    +---------------+    +---------------+    +---------------+
```

- **Master Host**: принимает подключения, проверяет права, компилирует SQL-запрос с помощью оптимизатора (ORCA или Postgres-based planner), рассылает план на выполнение сегментам и собирает итоговый результат. Сам мастер не хранит пользовательские данные.
- **Segment Hosts (Сегменты)**: независимые экземпляры PostgreSQL, хранящие свою отдельную порцию данных на локальных дисках и параллельно выполняющие вычисления.

### Наглядно: координатор и параллельные сегменты

```text
Coordinator → распределяет план и собирает нужный результат
Segments 1..N → выполняют части scan/join/aggregate
Motion → перенос промежуточных данных между участниками
```

Сеть и самый медленный сегмент могут ограничить весь запрос. Координатор не должен выполнять всю тяжёлую обработку данных один.

---

## 2. Стратегия распределения данных (DISTRIBUTED BY)

При создании любой таблицы в Greenplum инженер обязан явно задать политику распределения данных:

### 1. Хэш-распределение: `DISTRIBUTED BY (col1, col2)`
- Каждая строка отправляется на сегмент с номером: `hash(col1) % num_segments`.
- *Правило выбора ключа*: Колонка должна обладать высокой селективностью/кардинальностью (ID клиента, номер заказа, UUID).

### 2. Полная репликация: `DISTRIBUTED REPLICATED`
- Копия таблицы полностью дублируется на **каждом** сегменте кластера.
- *Когда использовать*: Для небольших таблиц справочников и измерений (`dim_categories`, `dim_regions` размером до сотен мегабайт). Это позволяет выполнять соединения `JOIN` локально на каждом сегменте без передачи данных по сети.

### 3. Случайное распределение: `DISTRIBUTED RANDOMLY`
- Распределение по круговому алгоритму Round-Robin. Гарантирует отсутствие перекоса, но делает невозможными локальные соединения (Collocated Joins).

### Наглядно: совпавшее распределение и локальный JOIN

```text
fact DISTRIBUTED BY(customer_id)
dim  DISTRIBUTED BY(customer_id) с совместимыми типами/распределением
Одинаковый ключ на подходящем сегменте → возможность локального JOIN
```

Физическая совместимость и план определяют, можно ли избежать Motion. Разные типы/выражения ключа и особые условия JOIN могут потребовать перераспределение.

---

## 3. Перекос данных (Data Skew): поиск и устранение

### Почему Skew катастрофичен в MPP:
В Greenplum запрос завершается только тогда, когда свою работу завершит **самый медленный сегмент**. Если из 100 млн строк таблицы 90 млн попали на Сегмент 1 (из-за популярного ключа `country_id = 0`), а на остальных 99 сегментах лежит по 100 тыс. строк:
- 99 сегментов завершат работу за 1 секунду и будут простаивать.
- 1 сегмент будет вычислять данные 20 минут и может переполнить свой локальный диск.

### Проверка перекоса таблицы через системную псевдоколонку `gp_segment_id`:
```sql
SELECT 
    gp_segment_id,
    COUNT(*) AS rows_on_segment
FROM fact_orders
GROUP BY gp_segment_id
ORDER BY rows_on_segment DESC;
```
Если разница между минимальным и максимальным значением превышает 10–15%, ключ распределения выбран неверно.

### Наглядно: хеш не разбивает повтор одного ключа

```text
Размеры сегментов: [900,50,50] млн строк
90% строк customer_id=42 → один hash destination
Больше сегментов без смены распределения → hot key остаётся
```

Различайте data skew и processing skew после фильтра/JOIN. Проверьте количество и объём строк, ключи NULL и распределение фактического промежуточного результата.

---

## 4. Движение данных в планах запросов (Motion Operators)

При анализе `EXPLAIN` в Greenplum особое внимание уделяется операторам **Motion** (сетевой обмен через Interconnect):

| Оператор Motion | Что происходит под капотом | Оценка эффективности |
|---|---|---|
| **Gather Motion** | Сегменты отправляют свои результаты мастеру для финальной отдачи клиенту | Нормально для финальной стадии запроса (с `LIMIT`) |
| **Broadcast Motion** | Каждый сегмент рассылает копию своих строк всем остальным сегментам кластера ($N \times N$) | Допустимо только для крошечных таблиц. Для больших таблиц — колоссальный оверхед на сеть |
| **Redistribute Motion** | Сегменты хэшируют строки по ключу соединения и пересылают друг другу | Применяется, если две большие таблицы соединяются по колонкам, не являющимся их ключами распределения |

### Локальное соединение (Collocated Join) — Святой Грааль Greenplum:
Если две большие таблицы распределены по **одному и тому же ключу** (`DISTRIBUTED BY (customer_id)`):
```sql
-- Обе таблицы fact_orders и dim_customers имеют одинаковый ключ дистрибьюции:
SELECT o.order_id, c.customer_name
FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id;
```
В плане запроса **нет ни одного оператора Motion**! Каждый сегмент соединяет только свои локальные данные. Производительность максимальна.

### Наглядно: три типа Motion

```text
Redistribute → каждый ряд к сегменту по новому ключу
Broadcast → вся малая сторона копируется сегментам
Gather → результат сегментов на один узел
```

Broadcast большой стороны умножает сетевой объём и память. Gather до тяжёлого расчёта может превратить распределённый запрос в узкое место.

---

## 5. Колоночное хранение и сжатие: Append-Only Columnar

По умолчанию таблицы создаются построчными (Heap Tables), что неэффективно для аналитических витрин на сотни миллиардов строк.

### Создание колоночной таблицы с современным сжатием ZSTD:
```sql
CREATE TABLE analytics.fact_sales_monthly (
    sale_id BIGINT,
    customer_id INT,
    order_date DATE,
    amount NUMERIC(12, 2),
    sku_code VARCHAR(32)
)
WITH (
    APPENDONLY = TRUE,
    ORIENTATION = COLUMN,
    COMPRESSTYPE = zstd,
    COMPRESSLEVEL = 5,
    BLOCKSIZE = 1048576       -- 1 МБ размер блока для высокой скорости последовательного чтения
)
DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (order_date) (
    START (DATE '2026-01-01') INCLUSIVE
    END (DATE '2027-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);
```

### Преимущества:
1. Коэффициент сжатия $4\times - 8\times$ по сравнению с классическим PostgreSQL.
2. При выполнении `SELECT SUM(amount) FROM ...` с диска читаются блоки только одной колонки `amount`.

---

### Наглядно: колонки, сжатие и зерно чтения

```text
Запросу нужны 3 из 100 колонок → AOCS читает нужные column blocks
Сжатие → меньше I/O, дополнительная работа CPU
Row-oriented layout → другой компромисс для обращений к строкам
```

Выбирайте формат и compression под workload и версию Greenplum. Append-optimized не означает отсутствие стоимости UPDATE/DELETE и обслуживания.

---

## 6. Системный словарь данных и мониторинг кластера

В отличие от стандартного PostgreSQL, в Greenplum системный словарь содержит специализированные представления для управления распределенной топологией:

### 6.1. Диагностика состояния сегментов: `gp_segment_configuration`
Главная таблица администратора и дата-инженера для контроля здоровья кластера:

```sql
SELECT 
    dbid, 
    content, 
    role,       -- 'p' (primary) или 'm' (mirror)
    preferred_role,
    mode,       -- 's' (synchronized) или 'r' (resynchronizing)
    status,     -- 'u' (up / работает) или 'd' (down / сбой)
    port, 
    hostname
FROM gp_segment_configuration
ORDER BY content, role;
```

> **Сигнал тревоги**: Если `role != preferred_role` или `status = 'd'`, произошел сбой первичного сегмента, и трафик переключился на резервное зеркало. Для восстановления синхронизации требуется системная утилита `gprecoverseg`.

### 6.2. Аудит политик распределения: `gp_distribution_policy`
Позволяет быстро найти все таблицы в базе, которые были ошибочно созданы со случайным распределением (`DISTRIBUTED RANDOMLY`):

```sql
SELECT 
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE 
        WHEN p.policytype = 'p' AND p.distkey IS NULL THEN 'DISTRIBUTED RANDOMLY'
        WHEN p.policytype = 'r' THEN 'DISTRIBUTED REPLICATED'
        ELSE 'DISTRIBUTED BY KEY'
    END AS distribution_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN gp_distribution_policy p ON p.localoid = c.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit');
```

### Наглядно: состояние кластера и одна отстающая часть

```text
Каталог → список primary/mirror segments и их состояние
Runtime metrics → очередь, spill, skew, network
Один деградировавший segment → длинный хвост всего запроса
```

Снимок каталога показывает конфигурацию, но не всю причину медленного выполнения. Сопоставляйте его с планом и метриками запроса.

---

## 7. Высокоскоростная параллельная загрузка: gpfdist и External Tables

Классическая команда `COPY` в Greenplum является бутылочным горлышком: все терабайты данных вынуждены проходить через один Master-узел, который парсит строки и рассылает их по сети сегментам.

Для параллельной загрузки Big Data используется утилита **`gpfdist`**:

```
                       [ Файловый сервер / ETL хост ]
                               (gpfdist:8081)
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         ▼ (HTTP параллельно)        ▼ (HTTP параллельно)        ▼ (HTTP параллельно)
   [ Segment 1 ]               [ Segment 2 ]               [ Segment 3 ]
   (Читает чанк 1)             (Читает чанк 2)             (Читает чанк 3)
```

1. На сервере с исходными CSV/текстовыми файлами запускается демон `gpfdist`:
   ```bash
   gpfdist -d /data/incoming -p 8081 -l /var/log/gpfdist.log &
   ```
2. В Greenplum создается **внешняя таблица (External Table)**:
   ```sql
   CREATE EXTERNAL TABLE ext_clickstream (
       event_id BIGINT,
       user_id BIGINT,
       event_time TIMESTAMPTZ,
       payload TEXT
   )
   LOCATION ('gpfdist://etl-host:8081/clickstream_*.csv')
   FORMAT 'CSV' (DELIMITER ',' HEADER)
   ENCODING 'UTF8';
   ```
3. Загрузка во внутреннюю колоночную таблицу выполняется за один запрос:
   ```sql
   INSERT INTO fact_clickstream SELECT * FROM ext_clickstream;
   ```
Каждый сегмент Greenplum устанавливает собственное независимое HTTP-соединение с `gpfdist` и выкачивает свою порцию данных параллельно со скоростью работы физической сети.

### Наглядно: параллельная загрузка вместо одного канала

```text
Файлы → несколько gpfdist endpoints → segments читают параллельно
Неверные строки → выбранная reject policy → отчёт и сверка
Загрузка закончилась → проверить count/keys/значения → публикация
```

Источник файлов, сеть и skew могут ограничить throughput. Быстрый ingest не доказывает полноту; сохраните возможность повторить чанк без дублей.

---

## 8. Официальные источники и документация вендора

Материалы основаны на официальной документации VMware Tanzu Greenplum 6 / 7 и Apache Greenplum (Cloudberry Database):
- [VMware Tanzu Greenplum Best Practices Guide](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/best_practices-intro.html) — официальное руководство по выбору ключей `DISTRIBUTED BY`, предотвращению Data Skew и настройке памяти сегментов (`gp_vmem_protect_limit`).
- [Greenplum Admin Guide: Defining Tables](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/admin_guide-ddl-ddl-table.html) — спецификация параметров Append-Only Columnar хранения (`ORIENTATION = COLUMN`, `COMPRESSTYPE = zstd`, `BLOCKSIZE`).
- [Greenplum Query Tuning & GPorca](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/admin_guide-query-topics-query-tuning.html) — оптимизатор ORCA, анализ операторов Motion в `EXPLAIN ANALYZE` (`Broadcast`, `Redistribute`, `Gather`) и минимизация сетевого оверхеда Interconnect.
- [Greenplum Toolkit: gp_toolkit Reference](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-gp_toolkit.html) — системные представления для поиска перекоса данных (`gp_toolkit.gp_skew_coefficients`).
- [Greenplum gpfdist Parallel File Server](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/utility_guide-ref-gpfdist.html) — архитектура и параметры протокола параллельной загрузки данных.
