# 09. Качество данных, валидация и дата-контракты (Data Quality & Data Contracts)

## 1. Введение: Почему упавший пайплайн лучше пайплайна с тихими ошибками

В Data Engineering существует фундаментальное правило: **"Падение пайплайна с ошибкой (fail-fast) во много раз дешевле, чем успешное завершение пайплайна, который записал некорректные данные"**.

Если пайплайн упал, дежурный инженер видит алерт и перезапускает расчет после исправления источника. Если пайплайн успешно завершился с битыми или дублированными данными, на этих данных строятся отчеты для руководства компании, переобучаются ML-модели и рассчитываются финансовые показатели. Обнаружение таких ошибок постфактум приводит к финансовым убыткам, искажению отчетности и потере доверия к дата-платформе.

### Наглядно: успех процесса и результат для бизнеса

```text
Pipeline success → опубликовано 0 заказов вместо ожидаемого набора
Pipeline failed на audit → неверный результат не опубликован
```

Статус выполнения не доказывает качество. Задайте критерий принятия данных и маршрут восстановления; решение о блокировке зависит от продукта.

---

## 2. Шесть измерений качества данных (Data Quality Dimensions)

Качество данных в индустрии оценивается по 6 классическим метрикам DAMA DMBOK:

| Измерение | Описание | Пример дефекта | Метод проверки |
| :--- | :--- | :--- | :--- |
| **Полнота (Completeness)** | Отсутствие пропусков (NULL, пустых строк) в критических полях | Поле `user_id` или `order_amount` равно NULL | `not_null` проверка |
| **Уникальность (Uniqueness)** | Отсутствие нежелательных дубликатов сущностей | Одинаковый `payment_id` записан дважды из-за ретрая Kafka | `unique` проверка первичного ключа |
| **Корректность (Validity)** | Соответствие формату, бизнес-правилам и диапазонам | Отрицательная цена товара, возраст 999 лет, статус 'OK' вместо 'completed' | `in_set`, регулярные выражения, min/max |
| **Актуальность (Timeliness / Freshness)** | Данные поступают вовремя с допустимой задержкой (SLA/SLO) | В таблицу за сегодняшний день последние данные пришли 12 часов назад | Проверка `max(loaded_at) >= now() - interval '2 hours'` |
| **Целостность (Consistency)** | Непротиворечивость данных между разными таблицами и системами | Заказ ссылается на `client_id`, которого нет в справочнике клиентов | `foreign_key`, референциальные проверки |
| **Точность (Accuracy)** | Соответствие данных реальным событиям в бизнес-системах | Сумма строк в чеке не сходится с итоговой суммой платежа | Кросс-табличная сверка (`sum(item_price) == order_total`) |

### Наглядно: разные проверки одного заказа

```text
id=NULL → completeness
id=42 дважды → uniqueness
amount=-10 при договоре amount>=0 → validity
Заказы не обновлялись 8 часов → freshness
customer_id не существует → consistency; сумма не соответствует источнику → accuracy
```

Valid значение может быть неточным: amount=100 проходит диапазон, хотя настоящий amount=120. Проверка точности требует подходящего эталона или бизнес-сверки.

---

## 3. Пирамида тестирования данных

Как и в классической разработке ПО, тестирование данных строится по уровням:

```
          / \
         /   \     Интеграционные и сквозные тесты (Cross-table reconciliation)
        /     \
       /-------\   Бизнес-логика и агрегации (Distribution, volume, anomaly detection)
      /         \
     /-----------\ Базовые тесты колонок (Not null, unique, accepted values, ranges)
    /             \
   /---------------\ Синтаксис и схема (Schema validation: типы, имена колонок, protobuf)
```

1. **Тесты схемы (Schema tests)**: Проверяют, что контракт структуры не нарушен. Поля не удалены, типы совпадают (`order_id: bigint`, `amount: numeric(12,2)`).
2. **Базовые тесты значений**: Проверяют каждую колонку изолированно (`order_id is not null`, `status in ('NEW', 'PAID', 'CANCELLED')`).
3. **Бизнес-тесты и аномалии**: Проверяют распределения и объемы. Например, объем дневной выручки не должен отклоняться более чем на 30% от медианы за последние 4 недели.
4. **Сквозная сверка (Reconciliation)**: Проверяет соответствие агрегатов между слоями Lakehouse (Bronze -> Silver -> Gold). Количество созданных заказов в сыром топике Kafka должно совпадать с количеством строк в финальной витрине.

### Наглядно: что обнаруживают уровни тестирования

```text
Schema pass: amount numeric
Value pass: amount>=0
Business fail: сумма не совпала с платежами
Reconciliation fail: пропущены исходные заказы
```

Равенство числа строк разных слоёв требуется только при соответствующем зерне и фильтрах. Dedup и агрегация закономерно меняют count; сравнивайте смысловые инварианты.

---

## 4. Инструменты тестирования: dbt Tests, Great Expectations, Soda

### 4.1. Тестирование в dbt (Generic & Singular Tests)

В dbt тестирование встроено в ядро фреймворка.

**Встроенные generic-тесты** описываются декларативно в `schema.yml`:

```yaml
version: 2

models:
  - name: fct_orders
    description: "Фактовая таблица подтвержденных заказов"
    columns:
      - name: order_id
        description: "Первичный ключ заказа"
        tests:
          - unique
          - not_null

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: order_status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'completed', 'returned']

      - name: total_amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
```

**Кастомные singular-тесты** (SQL-запросы, которые возвращают строки с ошибками; если строк 0 — тест пройден):

```sql
-- tests/assert_total_payment_amount_is_positive.sql
-- Тест проверяет, что сумма платежей по заказу не может быть отрицательной

select
    order_id,
    sum(payment_amount) as total_payment
from {{ ref('fct_payments') }}
group by order_id
having sum(payment_amount) < 0;
```

---

### 4.2. Great Expectations (GX)

Great Expectations — стандарт корпоративного уровня для валидации данных в Python-пайплайнах (Airflow, Spark, Pandas, Polars).

**Ключевые концепции GX:**
- **Expectation**: Правило проверки (например, `expect_column_values_to_be_unique`).
- **Expectation Suite**: Набор проверок для конкретной таблицы или датасета.
- **Batch Request**: Определение порции данных, которая подлежит валидации (например, партиция за дату `2026-09-20`).
- **Checkpoint**: Оркестратор запуска: объединяет данные, набор правил и действия при успехе/ошибке (отправка в Slack, генерация HTML-отчетов).
- **Data Docs**: Автоматически сгенерированная HTML-документация с визуальным статусом всех проверок.

**Пример пайплайна на Python:**

```python
import great_expectations as gx

# 1. Получаем контекст проекта
context = gx.get_context()

# 2. Создаем или получаем набор проверок
suite = context.add_or_update_expectation_suite(expectation_suite_name="orders_daily_suite")

# 3. Добавляем конкретные ожидания
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="order_amount",
        min_value=0.01,
        max_value=500000.00
    )
)
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1000,
        max_value=5000000
    )
)

# 4. Валидация датасета через Checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="orders_checkpoint",
    expectation_suite_name="orders_daily_suite"
)

results = checkpoint.run()

if not results["success"]:
    raise ValueError("Валидация качества данных не пройдена. Пайплайн остановлен.")
```

---

### 4.3. Soda Core

Легковесный CLI и Python-инструмент для проверки данных на SQL-базах и Spark через человекочитаемый синтаксис YAML:

```yaml
# checks_orders.yml
checks for fct_orders:
  - row_count > 1000
  - missing_count(order_id) = 0
  - duplicate_count(order_id) = 0
  - freshness(created_at) < 2h
  - schema:
      fail:
        when required column missing: [order_id, customer_id, order_amount, status]
        when wrong column type:
          order_amount: numeric
```

Запуск в Airflow BashOperator:
```bash
soda scan -d production_pg -c configuration.yml checks_orders.yml
```

### Наглядно: правило и реакция на результат

```text
Batch + набор правил → движок проверок → report
Pass → дальнейшая обработка
Fail → quarantine / остановка / уведомление по договору
```

dbt, GX, Soda и Deequ отличаются средой выполнения и API. Фиксируйте версии и проверяйте рабочую конфигурацию на небольшом батче; схема объясняет механизм, а не взаимозаменяемость API.

---

## 5. Дата-контракты (Data Contracts)

### 5.1. Что такое Data Contract и зачем он нужен

Историческая проблема Data Engineering: разработчики backend-сервиса меняют схему в PostgreSQL (удаляют колонку, меняют тип или логику статусов), не предупреждая команду данных. Ночью падают ETL-пайплайны, утром ломаются дашборды.

**Data Contract (Дата-контракт)** — это юридически и технически зафиксированное соглашение между поставщиком данных (Software Engineering) и потребителями данных (Data Engineers, Data Analysts, ML Engineers).

Контракт включает в себя 4 обязательные части:
1. **Схема (Schema)**: Имена полей, типы данных, обязательность (nullable).
2. **Семантика (Semantics)**: Бизнес-смысл каждого поля (например: "revenue включает НДС или нет?").
3. **SLA и качество (Quality SLA)**: Максимальная задержка поступления данных (freshness), допустимый процент ошибок, частота обновлений.
4. **Управление изменениями (Change Governance)**: Политика версионирования (SemVer) и правила внесения изменений без нарушения работы зависимых систем.

### 5.2. Пример спецификации Data Contract (Open Data Contract Standard)

```yaml
version: 3.1.0
kind: DataContract
metadata:
  name: orders_stream_contract
  owner: checkout_backend_team
  domain: sales
  version: 2.1.0

dataset:
  name: raw_orders
  format: parquet
  location: s3://company-lakehouse-bronze/sales/orders/

schema:
  - name: order_id
    type: string
    required: true
    description: "Глобальный UUID заказа, генерируется checkout-сервисом"
    unique: true

  - name: customer_id
    type: string
    required: true
    description: "Идентификатор покупателя из сервиса аутентификации"

  - name: total_cents
    type: integer
    required: true
    description: "Сумма заказа в копейках/центах без плавающей точки"
    logical_type: currency_cents

  - name: order_timestamp
    type: timestamp
    required: true
    description: "Момент фиксации заказа по UTC"

servicelevels:
  freshness:
    max_delay: "15 minutes"
  frequency: "streaming"
  availability: "99.9%"
```

### 5.3. Как дата-контракты внедряются технически

1. **Сдвиг влево (Shift-Left Testing)**: Контракты проверяются в CI/CD сервисов-источников. Если бэкенд-разработчик пытается удалить колонку, на которую завязан контракт, пайплайн сборки микросервиса выдает ошибку.
2. **Schema Registry**: При использовании Apache Kafka или Event Hub контракты валидируются на уровне сериализации (Avro / Protobuf / JSON Schema). Продюсер физически не может отправить сообщение, не соответствующее схеме.
3. **Автоматическое оповещение**: При изменении минорной версии контракта downstream-потребители получают уведомление через webhook или Slack.

### Наглядно: договор шире схемы

```text
Schema: amount decimal
Contract: amount в USD, без налогов, grain=order, владелец=Sales
Тип прежний, валюта стала EUR → schema pass, contract нарушен
```

Укажите правила совместимости, сроки, ответственность и обработку изменений. Новый nullable-атрибут не гарантирует совместимость со всеми потребителями.

---

## 6. Архитектурный паттерн WAP (Write-Audit-Publish)

Чтобы "битые" данные никогда не попадали к аналитикам, в современном Lakehouse (Apache Iceberg, Delta Lake) применяется паттерн **WAP**:

```
[ Сырые данные ]
       │
       ▼
1. WRITE: Запись данных в скрытую ветку (Branch) или staging-таблицу
       │
       ▼
2. AUDIT: Запуск проверок Great Expectations / dbt test на этой ветке
       │
   ├── Проверки упали? ──► [ ALERT в Slack + Откат ветки / Запись в Quarantine ]
   │
   └── Проверки прошли успешно?
       │
       ▼
3. PUBLISH: Fast-forward мердж ветки в основную таблицу (Main) за миллисекунды
```

В Apache Iceberg WAP реализован нативно через механизм снимков (snapshots) и веток (branches):
- Пайплайн делает коммит данных с тегом ветки `audit_branch`.
- Прогон тестов качества данных выполняется по состоянию ветки `audit_branch`.
- Если аудит пройден, вызывается системная процедура `cherrypick_snapshot` или `fast_forward_to`, делая данные видимыми в `main`.

### Наглядно: отделение записи от публикации

```text
Write → записать candidate v2
Audit → проверить schema, keys, totals, freshness
Pass → атомарно сделать v2 активной
Fail → сохранить v1 активной, candidate изолировать
```

Проверка должна относиться к той же версии данных, которая публикуется. Если между audit и publish набор меняется, гарантия теряется.

---

## 7. Типичные вопросы на собеседовании по Data Quality

### Вопрос 1: В чем разница между тестами на полноту и тестами на аномалии?
**Ответ**:
Тесты на полноту (Completeness) являются детерминированными тестами правил: мы проверяем условие `is not null` на каждой строке. Если условие нарушено, строка считается бракованной.
Тесты на аномалии являются статистическими: они оценивают агрегаты данных во времени (объем строк, среднее значение, распределение). Например, если в обычный вторник приходит 100 000 строк, а сегодня пришло 5 000 строк, все строки валидны по схеме и не содержат null, но произошла аномалия потери объема (volume anomaly).

### Вопрос 2: Что делать с дефектными строками: останавливать пайплайн или отбрасывать их?
**Ответ**:
Зависит от критичности данных и архитектурного соглашения:
- **Критичные финансовые данные**: Пайплайн должен немедленно упасть (Fail-Fast), так как частичная загрузка исказит финансовые итоги.
- **Потоковые пользовательские события (Clickstream)**: Недопустимо останавливать весь поток из-за 0.1% битых событий. Применяется паттерн **Dead Letter Queue (DLQ)** или **Карантинная таблица (Quarantine Table)**: валидные строки идут в аналитический слой, дефектные сохраняются в отдельную таблицу со статусом ошибки для дальнейшего анализа.

### Вопрос 3: Как организовать проверку уникальности на таблице в 10 миллиардов строк без долгого `COUNT(DISTINCT)`?
**Ответ**:
1. Проверять уникальность только на инкременте (новой партиции за сегодня), а не на всей исторической таблице.
2. Использовать приближенную оценку через HyperLogLog (`approx_count_distinct`), чтобы выявить всплеск дубликатов с минимальными затратами CPU.
3. Опираться на движок хранения: в ClickHouse использовать движок `ReplacingMergeTree`, в Delta Lake / Iceberg использовать конструкцию `MERGE INTO` с дедупликацией на этапе подготовки стейджинга.
