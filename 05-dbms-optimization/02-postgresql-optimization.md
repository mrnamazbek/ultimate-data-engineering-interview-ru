# Глубокая оптимизация PostgreSQL (PostgreSQL Performance Tuning)

PostgreSQL — самая популярная объектно-реляционная СУБД с открытым исходным кодом. В этом модуле собраны проверенные правила оптимизации запросов, выбора специализированных типов индексов, тюнинга параметров конфигурации ядра и управления автовакуумом.

---

## 1. Профессиональный анализ плана: `EXPLAIN (ANALYZE, BUFFERS)`

Базовый `EXPLAIN` показывает лишь теоретические предположения планировщика. Для реальной оптимизации всегда запускайте:

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
SELECT u.id, u.email, COUNT(o.id) AS total_orders
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.created_at >= '2026-01-01'
GROUP BY u.id, u.email;
```

### Ключевые метрики в выводе:
1. **Buffers: shared hit, read, dirtied, written**:
   - `shared hit`: страницы данных прочитаны напрямую из оперативной памяти (Shared Buffers). Скорость доступа — наносекунды.
   - `shared read`: страницы физически читались с диска (NVMe/SSD). Скорость — миллисекунды. Если `shared read` доминирует, в системе не хватает ОЗУ или отсутствует подходящий индекс.
2. **Sort Method: external merge Disk vs quicksort Memory**:
   - Если вы видите `external merge Disk: 15420kB`, это означает, что памяти `work_mem` не хватило, и СУБД сбросила промежуточную сортировку на диск. Увеличьте `work_mem`.
3. **Расхождение строк (Rows Estimation)**:
   - Если оценка планировщика `rows=10`, а фактически вернулось `actual rows=250000`, статистика устарела. Планировщик ошибочно выберет медленный `Nested Loop` вместо быстрого `Hash Join`. Необходимо выполнить `ANALYZE table_name;`.

---

## 2. Специализированные типы индексов в PostgreSQL

PostgreSQL предлагает богатейший набор структур индексов под разные типы нагрузок:

| Тип индекса | Под капотом | Лучшие сценарии | Ограничения |
|---|---|---|---|
| **B-Tree** (по умолчанию) | Сбалансированное дерево | Равенство (`=`), диапазоны (`<`, `>`, `BETWEEN`), сортировка (`ORDER BY`) | Не подходит для полнотекстового поиска и геометрии |
| **BRIN** (Block Range Index) | Хранит минимальное и максимальное значение диапазона страниц (блоков) | Огромные таблицы (> 100 млн строк), данные в которых физически упорядочены по времени (логи, события) | Занимает в 100 раз меньше места, чем B-Tree (1 МБ вместо 100 МБ), но работает только на упорядоченных вставках |
| **GIN** (Generalized Inverted Index) | Инвертированный индекс | Полнотекстовый поиск, массивы, поля типа `JSONB` с операторами `@>`, `?` | Медленная вставка и обновление, большой размер на диске |
| **GiST** (Generalized Search Tree) | Иерархическое дерево | Геопространственные данные (PostGIS), пересечения временных диапазонов (`tsrange`) | Медленнее на чтение, чем B-Tree |

### Продвинутые техники индексации:

#### Частичный индекс (Partial Index)
Индексирует не всю таблицу, а только строки, удовлетворяющие условию `WHERE`.
```sql
-- Таблица заказов содержит 100 млн завершенных строк и всего 50 000 в статусе 'PENDING'.
-- Индексировать 100 млн строк расточительно. Создаем частичный индекс:
CREATE INDEX idx_orders_pending ON orders(created_at)
WHERE status = 'PENDING';
-- Размер индекса составит 2 МБ вместо 3 ГБ!
```

#### Покрывающий индекс (Covering Index с INCLUDE)
Позволяет добиться `Index Only Scan` без добавления лишних колонок в дерево поиска:
```sql
-- Дерево строится только по email, но значения name и phone сохраняются в листовых узлах
CREATE INDEX idx_users_email_inc ON users(email) INCLUDE (name, phone);

-- Этот запрос читается на 100% из индекса без обращения к таблице:
SELECT name, phone FROM users WHERE email = 'client@example.com';
```

---

## 3. Настройка параметров конфигурации (`postgresql.conf`)

Стандартные настройки PostgreSQL после установки рассчитаны на минимальные ресурсы. Для рабочего сервера баз данных требуется скорректировать параметры:

| Параметр | Рекомендуемое значение | Описание и влияние |
|---|---|---|
| `shared_buffers` | 25% от всей RAM сервера | Основной пул оперативной памяти, используемый PostgreSQL для кэширования страниц данных |
| `effective_cache_size` | 50–75% от всей RAM | Оценка общего объема ОЗУ (включая файловый кэш ОС), доступного для кэширования. Помогает планировщику выбирать Index Scan вместо Seq Scan |
| `work_mem` | От 16MB до 128MB (осторожно) | Память на **одну операцию сортировки или хэш-таблицу**. Если в сложном запросе 5 соединений и 200 параллельных клиентов, умножение может исчерпать всю RAM |
| `maintenance_work_mem` | От 1GB до 2GB | Память под регламентные задачи: `VACUUM`, `CREATE INDEX`, добавление внешних ключей |
| `random_page_cost` | 1.1 (для NVMe/SSD) | По умолчанию стоит 4.0 (рассчитано на старые HDD). Для современных SSD снизьте до 1.1, чтобы планировщик охотнее использовал индексы |
| `checkpoint_completion_target` | 0.9 | Сглаживает пики дисковой записи контрольных точек, растягивая I/O на 90% времени между чекпоинтами |

---

## 4. Борьба с разрастанием таблиц (Table Bloat) и Autovacuum

Из-за архитектуры MVCC при каждом `UPDATE` создается новая версия строки, а старая становится мертвой (`dead tuple`). Процесс `autovacuum` обязан своевременно очищать эти строки, иначе размер таблицы разрастается, а запросы замедляются.

### Агрессивный тюнинг Autovacuum для нагруженных таблиц:
```sql
-- Настройка параметров для конкретной высоконагруженной таблицы
ALTER TABLE fact_events SET (
    autovacuum_vacuum_scale_factor = 0.05, -- запускать вакуум при 5% изменившихся строк (вместо дефолтных 20%)
    autovacuum_vacuum_cost_limit = 1000,   -- увеличиваем лимит ресурсов, выделяемых вакууму за один проход
    autovacuum_vacuum_cost_delay = 2       -- минимальная задержка между дисковыми операциями
);
```

### Мониторинг мертвых строк:
```sql
SELECT 
    schemaname,
    relname AS table_name,
    n_live_tup AS live_rows,
    n_dead_tup AS dead_rows,
    ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_row_pct,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC
LIMIT 10;
```

---

## 5. Мониторинг тяжелых запросов через `pg_stat_statements`

Расширение `pg_stat_statements` обязательно для любого продакшн-сервера:

```sql
-- Поиск топ-5 запросов, потребляющих больше всего процессорного времени сервера
SELECT 
    queryid,
    ROUND(total_exec_time::numeric / 1000, 2) AS total_sec,
    calls,
    ROUND(mean_exec_time::numeric, 2) AS avg_ms,
    ROUND((100.0 * total_exec_time / SUM(total_exec_time) OVER ())::numeric, 2) AS pct_of_total_time,
    query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;
```

---

## 6. Пул соединений: PgBouncer

В PostgreSQL каждое клиентское подключение обслуживается отдельным системным процессом операционной системы, потребляющим 5–10 МБ памяти. При 1000 прямых подключений сервер падает из-за постоянного переключения контекста процессора.

*Решение*: Развертывание **PgBouncer** в режиме `pool_mode = transaction`. Приложение держит тысячи легких клиентских соединений с PgBouncer, а сам пул соединений держит всего 30–50 постоянных соединений с реальным сервером PostgreSQL, обеспечивая максимальную пропускную способность.

---

## 7. Официальные источники и документация вендора

Все рекомендации и параметры в данном руководстве соответствуют официальной документации PostgreSQL (версии 14, 15, 16):
- [PostgreSQL Official Documentation: Performance Tips (Глава 14)](https://www.postgresql.org/docs/current/performance-tips.html) — разбор `EXPLAIN`, сбор статистики планировщика, оптимизация условий соединения.
- [PostgreSQL Official Documentation: Server Configuration — Resource Consumption (Глава 20)](https://www.postgresql.org/docs/current/runtime-config-resource.html) — спецификация параметров `shared_buffers`, `work_mem`, `effective_cache_size`, `maintenance_work_mem`.
- [PostgreSQL Official Documentation: Routine Vacuuming & Indexing (Глава 25)](https://www.postgresql.org/docs/current/routine-vacuuming.html) — механика очистки мертвых кортежей (dead tuples), предотвращение разрастания таблиц (bloat) и предотвращение Transaction ID Wraparound.
- [PostgreSQL Official Documentation: Index Types (Глава 64)](https://www.postgresql.org/docs/current/indexes-types.html) — внутреннее устройство B-Tree, BRIN, GIN, GiST и синтаксис покрывающих индексов `INCLUDE`.
- [PostgreSQL Module: pg_stat_statements](https://www.postgresql.org/docs/current/pgstatstatements.html) — профилирование выполнения запросов в оперативной памяти.

