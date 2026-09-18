# Продвинутый и аналитический SQL (Уровень Middle)

На позиции Middle Data Engineer требования к знанию SQL выходят далеко за рамки простых `JOIN` и `GROUP BY`. Инженер данных должен уверенно владеть оконными функциями, рассчитывать сложные накопительные метрики, работать с рекурсивными CTE и решать классические задачи обработки последовательностей (Gaps and Islands).

---

## 1. Анатомия оконных функций (Window Functions)

Оконная функция производит вычисления по набору строк (окну), логически связанному с текущей строкой, **не схлопывая** строки результата в одну (в отличие от обычной агрегации `GROUP BY`).

### Синтаксис
```sql
FUNCTION_NAME(column) OVER (
    PARTITION BY partition_col      -- деление на группы (аналог GROUP BY)
    ORDER BY sort_col [ASC|DESC]    -- порядок строк внутри группы
    ROWS|RANGE frame_specification  -- границы скользящего фрейма
)
```

### Ранжирующие функции: Сравнение
На собеседованиях всегда просят объяснить разницу между `ROW_NUMBER()`, `RANK()` и `DENSE_RANK()` на примере строк с одинаковыми значениями (ties).

Представим зарплаты сотрудников: `[100, 100, 80, 70]`.

| Функция | Логика работы | Результат для [100, 100, 80, 70] |
|---|---|---|
| `ROW_NUMBER()` | Присваивает строго уникальный последовательный номер каждой строке | `1, 2, 3, 4` |
| `RANK()` | Присваивает одинаковый ранг одинаковым значениям, но **пропускает** последующие номера рангов | `1, 1, 3, 4` (ранг 2 пропущен) |
| `DENSE_RANK()` | Присваивает одинаковый ранг одинаковым значениям **без пропусков** номеров | `1, 1, 2, 3` |
| `NTILE(k)` | Разбивает отсортированное окно на $k$ равных корзин/квантилей (например, разбиение на 4 квартиля для анализа оттока) | `1, 1, 2, 2` (при `NTILE(2)`) |

---

## 2. Накопительный итог (Running Total / Cumulative Sum)

Классическая задача: рассчитать прогресс накопления выручки день за днем как в реляционной СУБД, так и в PySpark.

### Решение на SQL
```sql
SELECT 
    sale_id,
    sale_date,
    amount,
    -- Накопительный итог по всей таблице
    SUM(amount) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    -- Накопительный итог с разбивкой по каждому магазину отдельно
    SUM(amount) OVER (
        PARTITION BY store_id 
        ORDER BY sale_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS store_running_total
FROM sales_data;
```

### Важное отличие `ROWS` от `RANGE`
- `ROWS BETWEEN ...` — оперирует физическими номерами строк.
- `RANGE BETWEEN ...` — оперирует значениями столбца сортировки. Если указать `SUM(amount) OVER (ORDER BY sale_date)` без явного `ROWS`, по стандарту применяется `RANGE UNBOUNDED PRECEDING`. Если в один день совершены 3 продажи, `RANGE` сложит их все вместе и выведет одинаковую суммарную цифру для всех трех строк, тогда как `ROWS` будет прибавлять строго строка за строкой.

### Решение на PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("RunningTotal").getOrCreate()

windowSpec = Window.partitionBy("store_id").orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_running = df.withColumn("store_running_total", _sum("amount").over(windowSpec))
```

---

## 3. Функции смещения: `LAG()` и `LEAD()`

Используются для сравнения значений текущей строки с предыдущей или следующей без использования тяжелых `SELF JOIN`.

### Кейс: Расчет прироста выручки день ко дню (Day-over-Day Growth)
```sql
WITH DailySales AS (
    SELECT 
        order_date,
        SUM(amount) AS daily_revenue
    FROM orders
    GROUP BY order_date
)
SELECT 
    order_date,
    daily_revenue,
    -- Значение за вчерашний день
    LAG(daily_revenue, 1) OVER (ORDER BY order_date) AS prev_day_revenue,
    -- Абсолютный прирост
    daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY order_date) AS diff_revenue,
    -- Относительный прирост в процентах
    ROUND(
        100.0 * (daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY order_date)) 
        / NULLIF(LAG(daily_revenue, 1) OVER (ORDER BY order_date), 0),
        2
    ) AS dod_growth_pct
FROM DailySales;
```

---

## 4. Скользящие средние (Moving Average)

Сглаживание суточных колебаний (например, недельное скользящее среднее 7-Day Moving Average):

```sql
SELECT 
    metric_date,
    active_users,
    ROUND(
        AVG(active_users) OVER (
            ORDER BY metric_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 
        2
    ) AS ma_7_days
FROM daily_metrics;
```

---

## 5. Рекурсивные CTE (Recursive Common Table Expressions)

Рекурсивный CTE позволяет обходить графовые и древовидные структуры (организационные иерархии, деревья категорий, маршруты).

### Кейс: Развертывание дерева подчинения сотрудников
Таблица `employees`: `emp_id`, `emp_name`, `manager_id`.
Требуется: вывести полный путь подчинения и уровень иерархии для каждого сотрудника.

```sql
WITH RECURSIVE OrgChart AS (
    -- 1. Якорный запрос (Anchor member): находим генерального директора (у него нет руководителя)
    SELECT 
        emp_id,
        emp_name,
        manager_id,
        1 AS level,
        CAST(emp_name AS VARCHAR(255)) AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- 2. Рекурсивная часть (Recursive member): находим подчиненных предыдущего уровня
    SELECT 
        e.emp_id,
        e.emp_name,
        e.manager_id,
        o.level + 1 AS level,
        CAST(o.path || ' -> ' || e.emp_name AS VARCHAR(255)) AS path
    FROM employees e
    INNER JOIN OrgChart o ON e.manager_id = o.emp_id
)
SELECT * FROM OrgChart ORDER BY level, emp_id;
```

---

## 6. Задача о «пропусках и островках» (Gaps and Islands)

Классическая задача на позиции Middle / Senior Data Engineer.
- **Островки (Islands)**: найти непрерывные диапазоны дат (например, непрерывные серии входов пользователя на сайт).
- **Пропуски (Gaps)**: найти утерянные диапазоны идентификаторов или дат.

### Алгоритм нахождения островков (разница между датой и ROW_NUMBER)
```sql
-- Таблица активности пользователей: user_id, active_date
WITH GroupedActivity AS (
    SELECT 
        user_id,
        active_date,
        -- Если даты идут подряд, active_date и row_number растут с одинаковым шагом.
        -- Их разность (active_date - row_number) будет постоянной величиной для непрерывного острова!
        active_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY active_date) * INTERVAL '1 day') AS island_id
    FROM user_activity
)
SELECT 
    user_id,
    MIN(active_date) AS streak_start_date,
    MAX(active_date) AS streak_end_date,
    COUNT(*) AS streak_length_days
FROM GroupedActivity
GROUP BY user_id, island_id
HAVING COUNT(*) >= 3 -- серии от 3 дней и дольше
ORDER BY user_id, streak_start_date;
```

---

## 7. Многоуровневые группировки: GROUPING SETS, ROLLUP и CUBE

При построении витрин отчетности часто требуется посчитать итоги на разных уровнях детализации без использования ресурсоемких объединений через `UNION ALL`.

```sql
-- Подсчет продаж по регионам, категориям, а также общих подытогов
SELECT 
    region,
    category,
    SUM(revenue) AS total_revenue,
    GROUPING(region) AS is_region_subtotal,
    GROUPING(category) AS is_category_subtotal
FROM sales
GROUP BY ROLLUP(region, category);
-- ROLLUP генерирует: (region, category), (region), () - глобальный итог
```
