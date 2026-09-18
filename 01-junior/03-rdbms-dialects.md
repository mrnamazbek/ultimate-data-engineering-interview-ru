# Особенности промышленных СУБД: Oracle, MS SQL Server и PostgreSQL (Уровень Junior)

В корпоративных проектах и традиционном банковском/телеком-секторе инженеры данных регулярно работают с промышленными СУБД: **Oracle Database**, **Microsoft SQL Server** и **PostgreSQL**. На технических собеседованиях часто проверяют знание специфики конкретных диалектов, оптимизацию массовой загрузки данных и базовую диагностику производительности.

---

## 1. Специфика Oracle Database

### Основные концепции и объекты
1. **ROWID против ROWNUM**:
   - `ROWID` — псевдоколонка, представляющая собой уникальный физический адрес строки на диске (номер файла данных, блока и строки в блоке). Выборка по `ROWID` — самый быстрый способ доступа к данным в Oracle.
   - `ROWNUM` — временный порядковый номер, присваиваемый строке в момент ее извлечения из набора до выполнения `ORDER BY`.
2. **Force View**:
   - Представление, создаваемое с директивой `CREATE FORCE VIEW`. Позволяет создать View, даже если базовые таблицы еще не существуют (полезно при развертывании сложных DDL-скриптов и циклических зависимостях).
3. **Inline View**:
   - Подзапрос во фразе `FROM` основного запроса.

### Функции Oracle против стандартов ANSI SQL
- `NVL(expr1, expr2)` — заменяет `NULL` на `expr2`. Является аналогом ANSI SQL `COALESCE(expr1, expr2)`.
- `NVL2(expr1, expr2, expr3)` — если `expr1` не `NULL`, возвращает `expr2`; если `expr1` равен `NULL`, возвращает `expr3`.
- `DECODE(col, val1, res1, val2, res2, default_res)` — устаревшая проприетарная функция ветвления Oracle, заменяемая современным `CASE WHEN`.

### Пакетная обработка в PL/SQL (Bulk Operations)
Для предотвращения частого переключения контекста между SQL-движком и PL/SQL-интерпретатором используются конструкции массовой загрузки:
```sql
DECLARE
    TYPE t_emp_ids IS TABLE OF employees.id%TYPE;
    v_emp_ids t_emp_ids;
BEGIN
    -- BULK COLLECT: извлечение массива данных за один контекстный переход
    SELECT id BULK COLLECT INTO v_emp_ids 
    FROM employees 
    WHERE department_id = 10;

    -- FORALL: пакетное выполнение DML над коллекцией
    FORALL i IN 1..v_emp_ids.COUNT
        UPDATE employees 
        SET bonus = bonus * 1.1 
        WHERE id = v_emp_ids(i);
        
    COMMIT;
END;
```

### Диагностика производительности Oracle (Performance Check)
Инженер данных должен уметь базово диагностировать «зависшие» или медленные запросы через динамические системные представления (`v$`):

```sql
-- 1. Поиск самых медленных запросов по времени выполнения (Elapsed Time)
SELECT *
FROM (
    SELECT 
        sql_id,
        ROUND(elapsed_time / 1000000, 2) AS elapsed_seconds,
        executions,
        ROUND((elapsed_time / 1000000) / NULLIF(executions, 0), 2) AS avg_sec_per_exec,
        sql_text
    FROM v$sql
    ORDER BY elapsed_time DESC
)
WHERE ROWNUM <= 10;

-- 2. Проверка активных блокировок и ожиданий (Locks & Waits)
SELECT 
    s.sid,
    s.serial#,
    s.username,
    s.status,
    s.event,
    s.seconds_in_wait
FROM v$session s
WHERE s.type != 'BACKGROUND' 
  AND s.status = 'ACTIVE';
```

---

## 2. Специфика Microsoft SQL Server (T-SQL)

### Переменные: локальные и системные
- **Локальные переменные**: объявляются через `DECLARE @variable_name datatype` и действуют только в рамках текущего батча (batch):
  ```sql
  DECLARE @cutoff_date DATE = '2024-01-01';
  SELECT * FROM orders WHERE order_date >= @cutoff_date;
  ```
- **Системные функции и переменные**: начинаются с двух символов `@`:
  - `@@ROWCOUNT` — возвращает количество строк, затронутых предыдущей инструкцией.
  - `@@ERROR` — код ошибки последней операции T-SQL (или 0 в случае успеха).
  - `@@IDENTITY` — последнее значение автоинкрементного ключа в рамках сессии.

### Временные таблицы против табличных переменных
- `#temp_table` (Локальная временная таблица): создается в системной базе `tempdb`, доступна только текущей сессии. Поддерживает создание индексов, сбор статистики и эффективна для больших наборов данных (> 10 000 строк).
- `##temp_table` (Глобальная временная таблица): доступна всем пользователям и сессиям до закрытия создавшего ее соединения.
- `@table_variable` (Табличная переменная): хранится в памяти (при переполнении сбрасывается в `tempdb`), не имеет статистики для оптимизатора, подходит только для малых объемов (< 100 строк).

### Быстрая загрузка больших объемов данных (Bulk Load)
На собеседованиях часто задают вопрос: *«Как быстро загрузить 50 миллионов строк в SQL Server?»*

1. Использовать команду `BULK INSERT` или CLI-утилиту `bcp`.
2. Установить модель восстановления базы данных в `BULK_LOGGED` или `SIMPLE` (минимизирует запись в transaction log).
3. Временно отключить некластерные индексы и внешние ключи на таблице приемника.
4. Выполнить вставку с опцией `TABLOCK` (блокировка всей таблицы позволяет обойти конкурентные блокировки страниц).
5. Включить индексы заново и перестроить их (`REBUILD`).
6. Обновить статистику: `UPDATE STATISTICS target_table;`.

```sql
BULK INSERT staging_transactions
FROM 'C:\data\transactions_2024.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
```

---

## 3. Специфика PostgreSQL для Data Engineer

PostgreSQL является де-факто стандартом среди реляционных СУБД в современной экосистеме данных (Greenplum, Redshift, YugabyteDB построены на базе PG-протокола).

### Механизм MVCC (Multi-Version Concurrency Control)
- При выполнении `UPDATE` PostgreSQL физически не изменяет старую строку, а помечает ее как «мертвую» (dead tuple) и вставляет новую версию строки.
- При выполнении `DELETE` строка помечается как удаленная, но место на диске не освобождается.
- Процесс `VACUUM` сканирует страницы данных и очищает место, занятое мертвыми строками, предотвращая разрастание таблиц (table bloat). `VACUUM ANALYZE` дополнительно обновляет статистику распределения значений для оптимизатора.

### Конструкция UPSERT (ON CONFLICT)
Стандартный паттерн при загрузке данных (Idempotent Load):
```sql
INSERT INTO dim_customers (customer_id, full_name, email, updated_at)
VALUES (101, 'Ivan Ivanov', 'ivan@example.com', NOW())
ON CONFLICT (customer_id) 
DO UPDATE SET 
    full_name = EXCLUDED.full_name,
    email = EXCLUDED.email,
    updated_at = NOW();
```

### Полуструктурированные данные: JSONB и GIN-индексы
В отличие от обычного текстового `JSON`, тип `JSONB` сохраняет данные в бинарном разобранном виде:
```sql
-- Поиск по атрибуту внутри JSONB
SELECT * 
FROM raw_events 
WHERE payload @> '{"event_type": "purchase"}';

-- Создание инвертированного GIN-индекса для ускорения поиска по ключам JSONB
CREATE INDEX idx_raw_events_payload ON raw_events USING GIN (payload);
```
