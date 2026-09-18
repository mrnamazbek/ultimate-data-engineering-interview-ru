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

## 6. Официальные источники и документация вендора

Материалы основаны на официальной документации VMware Tanzu Greenplum 6 / 7 и Apache Greenplum (Cloudberry Database):
- [VMware Tanzu Greenplum Best Practices Guide](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/best_practices-intro.html) — официальное руководство по выбору ключей `DISTRIBUTED BY`, предотвращению Data Skew и настройке памяти сегментов (`gp_vmem_protect_limit`).
- [Greenplum Admin Guide: Defining Tables](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/admin_guide-ddl-ddl-table.html) — спецификация параметров Append-Only Columnar хранения (`ORIENTATION = COLUMN`, `COMPRESSTYPE = zstd`, `BLOCKSIZE`).
- [Greenplum Query Tuning & GPorca](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/admin_guide-query-topics-query-tuning.html) — оптимизатор ORCA, анализ операторов Motion в `EXPLAIN ANALYZE` (`Broadcast`, `Redistribute`, `Gather`) и минимизация сетевого оверхеда Interconnect.
- [Greenplum Toolkit: gp_toolkit Reference](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-gp_toolkit.html) — системные представления для поиска перекоса данных (`gp_toolkit.gp_skew_coefficients`).

