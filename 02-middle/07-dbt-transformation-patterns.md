# Шаблоны трансформации данных в dbt (Data Build Tool)

Инструмент **dbt (data build tool)** стал стандартом де-факто для слоя трансформации данных (T в парадигме ELT) в современных аналитических хранилищах (Snowflake, BigQuery, ClickHouse, Databricks, PostgreSQL). На собеседованиях уровня Middle и Senior знание dbt проверяется как обязательный навык разработки витрин данных.

---

## 1. Архитектура слоев dbt: Staging, Intermediate, Marts

Правильная организация dbt-проекта разделяет код на изолированные уровни абстракции:

```text
               Сырые таблицы (Sources)
                          |
              +-----------v-----------+
              |    Слой Staging       |  (stg_orders.sql, stg_users.sql)
              |  - 1-к-1 к источнику  |  - Переименование полей
              |  - Очистка типов      |  - Базовая фильтрация
              +-----------+-----------+
                          |
              +-----------v-----------+
              |   Слой Intermediate   |  (int_customer_orders.sql)
              |  - Бизнес-логика      |  - Сложные JOIN-ы
              |  - Обогащение данных  |  - Не виден конечным BI
              +-----------+-----------+
                          |
              +-----------v-----------+
              |       Слой Marts      |  (fct_sales.sql, dim_customers.sql)
              |  - Модели Кимбалла    |  - Таблицы фактов и измерений
              |  - Витрины для BI     |  - Высокая скорость чтения
              +-----------------------+
```

### Правила именования моделей:
- `stg_[источник]__[сущность]` — модели слоя Staging (например, `stg_stripe__charges.sql`).
- `int_[сущность]__[действие]` — промежуточные модели (например, `int_orders__pivoted_payments.sql`).
- `fct_[сущность]` — таблицы фактов (например, `fct_daily_revenue.sql`).
- `dim_[сущность]` — таблицы измерений (например, `dim_customers.sql`).

---

## 2. Типы материализаций (Materializations)

Директива `{{ config(materialized='...') }}` определяет, как физически компилируется модель в базе данных:

| Материализация | Что создается в СУБД | Плюсы | Минусы | Когда применять |
|---|---|---|---|---|
| **view** (по умолчанию) | `CREATE VIEW` | Быстрый запуск `dbt run`, всегда свежие данные, не тратит место на диске | Повторяющиеся тяжелые вычисления при каждом обращении | Слой Staging и простые промежуточные расчеты |
| **table** | `CREATE TABLE AS SELECT` | Быстрое чтение аналитиками, возможность индексации/кластеризации | Длительное время пересчета при каждом запуске | Слой Marts для таблиц небольшого и среднего размера |
| **incremental** | Вставка только новых/измененных строк | Высокая скорость работы на терабайтных фактах, экономия CPU | Сложная отладка, риск расхождения данных при ошибках логики | Сверхбольшие таблицы фактов (логи, транзакции) |
| **ephemeral** | CTE (`WITH ... AS`) | Не создает объектов в базе данных, повторное использование SQL | Нельзя протестировать отдельно через `dbt test` | Очень легкие переиспользуемые фрагменты логики |

---

## 3. Инкрементальные модели (Incremental Models)

Инкрементальная модель пересчитывает только строки, появившиеся с момента предыдущего запуска dbt:

```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

WITH raw_transactions AS (
    SELECT * FROM {{ ref('stg_payments__transactions') }}
    
    {% if is_incremental() %}
        -- Фильтр применяется только при повторных инкрементальных запусках!
        -- При первом запуске или dbt run --full-refresh условие игнорируется
        WHERE updated_at >= (SELECT MAX(updated_at) - INTERVAL '3 day' FROM {{ this }})
    {% endif %}
)

SELECT 
    transaction_id,
    user_id,
    amount,
    currency,
    status,
    updated_at
FROM raw_transactions;
```

### Стратегии инкрементальной загрузки:
1. **merge** (по умолчанию в Snowflake/BigQuery): выполняет SQL-конструкцию `MERGE INTO target USING source ON target.unique_key = source.unique_key`. Обновляет изменившиеся строки и вставляет новые.
2. **delete+insert**: атомарно удаляет строки с совпадающими ключами и вставляет свежие. Идеально для баз данных без поддержки MERGE (PostgreSQL).
3. **insert_overwrite**: заменяет целиком партицию (например, заменяет партицию за дату). Самый быстрый и дешевый способ в облачных DWH (BigQuery / Databricks).

---

## 4. Тестирование и контракты данных в dbt

dbt превращает тестирование данных в обязательную часть CI/CD конвейера:

### Стандартные встроенные тесты (`schema.yml`):
```yaml
version: 2

models:
  - name: fct_orders
    description: "Витрина фактов оформленных заказов"
    columns:
      - name: order_id
        description: "Первичный суррогатный ключ заказа"
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'delivered', 'returned']
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

### Сингулярные тесты (Singular Tests):
Пользовательский SQL-запрос в папке `tests/`. Если запрос возвращает хотя бы **одну строку**, тест считается **провалившимся**:
```sql
-- tests/assert_total_amount_is_positive.sql
-- Выручка по заказу не может быть отрицательной
SELECT order_id, amount
FROM {{ ref('fct_orders') }}
WHERE amount < 0;
```

---

## 5. Снимки состояния (dbt Snapshots / SCD Type 2)

dbt автоматизирует реализацию медленно меняющихся измерений (SCD Type 2) без необходимости ручного написания сложных триггеров:

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

SELECT * FROM {{ source('crm', 'raw_customers') }}

{% endsnapshot %}
```
dbt автоматически добавит служебные колонки `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id` и будет обновлять историю изменений при каждом запуске команды `dbt snapshot`.

---

## 6. Официальные источники и документация

- [dbt Official Documentation: Best Practices](https://docs.getdbt.com/best-practices) — организация структуры проекта, слои Staging/Intermediate/Marts.
- [dbt Documentation: Materializations](https://docs.getdbt.com/docs/build/materializations) — спецификация типов материализации и параметров конфигурации.
- [dbt Documentation: Incremental Models](https://docs.getdbt.com/docs/build/incremental-models) — тонкости макроса `is_incremental()` и выбор инкрементальных стратегий.
