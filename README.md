# Ultimate Data Engineering Interview Preparation (Полное руководство по подготовке к собеседованиям по Data Engineering)

Данный репозиторий представляет собой систематизированную базу знаний для подготовки к техническим собеседованиям на позиции **Junior**, **Middle** и **Senior Data Engineer**, а также **Database Developer**.

Все материалы переведены, переработаны и дополнены практическими примерами из реальной практики больших данных (Big Data), распределенных систем, облачных платформ и корпоративных хранилищ данных.

Репозиторий полностью синхронизирован и обогащен практиками официального стека навыков **Antigravity v17.5.0** (108 профильных навыков по Data Engineering, СУБД, Python, ML, распределенным системам и подготовке к интервью).

---

## 1. Матрица компетенций Data Engineer по грейдам

| Направление | Junior Data Engineer | Middle Data Engineer | Senior / Lead Data Engineer |
|---|---|---|---|
| **SQL и Базы данных** | DDL/DML, типы JOIN, группировки, логика NULL, индексы B-Tree, нормализация 1NF-3NF. | Оконные функции, накопительные суммы, рекурсивные CTE, оптимизация планов (`EXPLAIN`), транзакции. | Внутреннее устройство СУБД, LSM-деревья, распределенные шарды, партиционирование, NoSQL (Cassandra, DynamoDB). |
| **Моделирование DWH** | Понимание таблиц фактов и измерений, схема «Звезда». | Методология Кимбалла, схема «Снежинка», SCD 1–6, dbt-моделирование, staging/intermediate/marts. | Корпоративная архитектура данных, Data Mesh, Data Governance, стандарты Medallion, Lakehouse. |
| **Распределенная обработка** | Основы Spark (RDD vs DataFrame), базовые трансформации. | Тюнинг Catalyst Optimizer, `repartition` vs `coalesce`, кэширование, устранение OOM, Polars vs Pandas. | Spark Internals: Shuffle write/read, Data Skew, Salting, Adaptive Query Execution (AQE), Streaming. |
| **Оркестрация и качество** | Написание простых Airflow DAG, запуск операторов. | Сенсоры (`reschedule`), XCom, dbt tests, Great Expectations, Soda Core, алертинг и мониторинг. | Динамическая генерация DAG, Data Contracts, Shift-Left тестирование данных, SLA/SLO конвейеров. |
| **Хранилища и Lakehouse** | Работа с CSV, JSON, Parquet в Data Lake. | Партиционирование таблиц, Snowflake (виртуальные склады, clustering, Time Travel), BigQuery. | Форматы Apache Iceberg, Delta Lake: Time Travel, скрытое партиционирование, WAP-паттерн, компактизация. |
| **Потоковая обработка** | Понимание разницы между Batch и Streaming. | Чтение из Kafka, базовые микробатчи в Spark Structured Streaming. | Архитектуры Lambda/Kappa, Exactly-Once семантика, водяные знаки (Watermarks), Flink, CDC (Debezium). |
| **AI/ML и Векторные базы** | Базовое понимание жизненного цикла данных. | Подготовка датасетов для обучения, пайплайны фичей. | Векторные СУБД (pgvector, Milvus, Qdrant), алгоритмы HNSW/IVF, квантование (PQ), масштабирование RAG. |
| **System Design** | Роль отдельных сервисов в конвейере. | Проектирование типовых ETL/ELT конвейеров с контролем качества данных. | Сквозное проектирование платформ данных на сотни терабайт, отказоустойчивость, расчет TCO и FinOps. |

---

## 2. Структура репозитория и модули подготовки

### 01-junior: Фундамент и базовые концепции
1. [01-sql-basics.md](01-junior/01-sql-basics.md) — Порядок выполнения SQL, разница `DELETE` / `TRUNCATE` / `DROP`, виды `JOIN`, `UNION ALL`, логика `NULL`, подзапросы и поиск дубликатов.
2. [02-database-fundamentals.md](01-junior/02-database-fundamentals.md) — Принципы транзакций ACID, уровни изоляции и аномалии, кластерные и некластерные индексы, нормализация 1NF–3NF, денормализация.
3. [03-rdbms-dialects.md](01-junior/03-rdbms-dialects.md) — Специфика промышленных СУБД: Oracle (ROWNUM, packages, PL/SQL bulk), MS SQL Server (T-SQL, временные таблицы, BCP), PostgreSQL (MVCC, VACUUM, JSONB).
4. [04-python-for-data-engineers.md](01-junior/04-python-for-data-engineers.md) — Сложность алгоритмов Big-O для коллекций Python, потоковое чтение терабайтных файлов через генераторы, безопасная работа с базами данных (DB API 2.0).
5. [05-data-engineering-core-concepts.md](01-junior/05-data-engineering-core-concepts.md) — Сравнение Batch, Stream и Real-Time, архитектуры ETL против ELT, эволюция хранилищ (DWH, Data Lake, Lakehouse), суррогатные ключи.

### 02-middle: Практическая разработка, аналитический SQL и конвейеры
1. [01-advanced-sql-analytical.md](02-middle/01-advanced-sql-analytical.md) — Анатомия оконных функций, ранжирование (`ROW_NUMBER` vs `RANK` vs `DENSE_RANK`), накопительные итоги (Running Total), рекурсивные CTE, задача Gaps and Islands.
2. [02-data-modeling-dwh.md](02-middle/02-data-modeling-dwh.md) — Размерное моделирование Кимбалла, классификация фактов (аддитивные, полуаддитивные, безфакторные), типы измерений, глубокий разбор SCD Type 1–6.
3. [03-apache-spark-pyspark.md](02-middle/03-apache-spark-pyspark.md) — Архитектура Spark, Driver и Executors, Catalyst Optimizer, узкие и широкие трансформации, `repartition` vs `coalesce`, Vectorized Pandas UDF.
4. [04-workflow-orchestration-airflow.md](02-middle/04-workflow-orchestration-airflow.md) — Компоненты Airflow, жизненный цикл TaskInstance, тонкости сенсоров (`poke` vs `reschedule`), антипаттерны XCom, сравнение CeleryExecutor и KubernetesExecutor.
5. [05-cloud-data-engineering.md](02-middle/05-cloud-data-engineering.md) — Облачный стек: Azure Data Factory (Integration Runtimes, триггеры), ADLS Gen2, Google BigQuery (разделение вычислений и диска, партиционирование, кластеризация).
6. [06-production-troubleshooting.md](02-middle/06-production-troubleshooting.md) — Разбор боевых инцидентов: Out Of Memory в Spark (Driver vs Executor), перекос данных, обеспечение идемпотентности, борьба с мелкими файлами, сдвиг схемы (Schema Drift).
7. [07-dbt-transformation-patterns.md](02-middle/07-dbt-transformation-patterns.md) — Практика dbt: архитектура слоев (staging -> intermediate -> marts), материализации (view, table, incremental), стратегии инкрементов (`merge`, `append`, `delete+insert`), Jinja/макросы.
8. [08-snowflake-cloud-dwh.md](02-middle/08-snowflake-cloud-dwh.md) — Архитектура Snowflake: трехуровневое разделение (Storage, Virtual Warehouses, Cloud Services), микропартиции и Data Clustering, непрерывная загрузка Snowpipe, Time Travel и Fail-safe, Zero-Copy Cloning.
9. [09-data-quality-and-contracts.md](02-middle/09-data-quality-and-contracts.md) — Качество данных: 6 измерений качества (DAMA), пирамида тестирования, dbt-тесты, Great Expectations, Soda Core, спецификация Data Contracts и паттерн Write-Audit-Publish (WAP).
10. [10-polars-high-performance-python.md](02-middle/10-polars-high-performance-python.md) — Высокопроизводительный Python: архитектура Polars поверх Apache Arrow (Rust, SIMD, zero-copy), Expressions API, LazyFrame, оптимизации планов запросов, потоковый движок (`collect(streaming=True)`).

### 03-senior: Архитектура, распределенные системы и масштабирование
1. [01-distributed-systems-spark-internals.md](03-senior/01-distributed-systems-spark-internals.md) — Низкоуровневое устройство Spark: архитектура памяти JVM, механика Shuffle Write/Read, Spill to Disk, стратегии JOIN, техника Key Salting, Adaptive Query Execution (AQE).
2. [02-storage-formats-lakehouse.md](03-senior/02-storage-formats-lakehouse.md) — Форматы таблиц нового поколения: Apache Iceberg, Delta Lake, дерево метаданных, скрытое партиционирование, Time Travel, кейс миграции и архивации 40 ТБ данных.
3. [03-streaming-realtime-architectures.md](03-senior/03-streaming-realtime-architectures.md) — Сравнение архитектур Lambda и Kappa, гарантии доставки (At-least-once, Exactly-Once), Event Time против Processing Time, водяные знаки (Watermarking), оконные стратегии.
4. [04-algorithms-data-engineering.md](03-senior/04-algorithms-data-engineering.md) — Вероятностные структуры данных: фильтр Блума, HyperLogLog, консистентное хеширование, устройство LSM-деревьев против B+ Tree.
5. [05-system-design-data-platforms.md](03-senior/05-system-design-data-platforms.md) — Фреймворк прохождения секции System Design, медальонная архитектура (Bronze -> Silver -> Gold), принципы Data Mesh, Data Lineage и безопасность персональных данных (PII).
6. [06-mlops-for-data-engineers.md](03-senior/06-mlops-for-data-engineers.md) — Инженерия данных для ML/AI: Feature Stores (Offline vs Online), Point-in-time correctness, пайплайны эмбеддингов, мониторинг Data Drift.
7. [07-nosql-distributed-databases.md](03-senior/07-nosql-distributed-databases.md) — Распределенные NoSQL СУБД: переход к Query-First моделированию, архитектура Cassandra и DynamoDB, составные ключи (Partition vs Sort Key), устранение Hot Partitions, Single-Table Design, LSM-деревья, Tombstones и Tunable Consistency ($R + W > N$).
8. [08-vector-databases-and-rag.md](03-senior/08-vector-databases-and-rag.md) — Векторные СУБД и RAG: метрики расстояния (Cosine, L2, Dot Product), алгоритмы ANN (IVFFlat, HNSW, Product Quantization), пайплайны чанкинга и эмбеддингов, пре- и пост-фильтрация, гибридный поиск (Dense + Sparse/BM25), реализация на `pgvector`.

### 04-interview-cheatsheets: Экспресс-шпаргалки и подготовка к офферу
1. [sql-speedrun-qa.md](04-interview-cheatsheets/sql-speedrun-qa.md) — Блиц-справочник: 50 вопросов и ответов по SQL и СУБД для быстрого повторения перед интервью.
2. [spark-speedrun-qa.md](04-interview-cheatsheets/spark-speedrun-qa.md) — Блиц-справочник: 30 ключевых вопросов и ответов по Apache Spark и распределенной обработке.
3. [real-interview-experience.md](04-interview-cheatsheets/real-interview-experience.md) — Практика прохождения собеседований: декодирование вакансий (6 линз JD), самопрезентация по модели STAR с Earned Secrets, каверзные вопросы с подвохом, поведенческие кейсы и стратегия переговоров по зарплате.

### 05-dbms-optimization: Оптимизация и тюнинг производительности СУБД
1. [01-universal-sql-optimization.md](05-dbms-optimization/01-universal-sql-optimization.md) — Универсальные правила: чтение `EXPLAIN`, SARGable предикаты, курсорная пагинация (Keyset Pagination) против `OFFSET`, устранение антипаттерна N+1.
2. [02-postgresql-optimization.md](05-dbms-optimization/02-postgresql-optimization.md) — Глубокая оптимизация PostgreSQL: `EXPLAIN (ANALYZE, BUFFERS)`, частичные и покрывающие индексы, BRIN, тюнинг `shared_buffers`/`work_mem`, борьба с Table Bloat и autovacuum, пулер соединений PgBouncer.
3. [03-oracle-optimization.md](05-dbms-optimization/03-oracle-optimization.md) — Оптимизация Oracle Database: CBO, сбор статистики, гистограммы, подсказки оптимизатора (Hints), анализ отчетов AWR и событий ожидания (`db file sequential read`).
4. [04-mysql-optimization.md](05-dbms-optimization/04-mysql-optimization.md) — Оптимизация MySQL & InnoDB: организация кластерного индекса, предотвращение Page Splits при использовании UUID, настройка `innodb_buffer_pool_size`, анализ Slow Query Log через `pt-query-digest`.
5. [05-greenplum-mpp-optimization.md](05-dbms-optimization/05-greenplum-mpp-optimization.md) — Оптимизация Greenplum (MPP): архитектура Master-Segment, стратегия распределения `DISTRIBUTED BY`, устранение Data Skew, ликвидация операторов Motion (Collocated Joins), колоночное сжатие ZSTD.
6. [06-vertica-mpp-optimization.md](05-dbms-optimization/06-vertica-mpp-optimization.md) — Оптимизация OpenText Vertica: архитектура проекций (Projections), кодирование данных (RLE, DELTAVAL), Tuple Mover (WOS -> ROS), предотвращение ROS pushback через `COPY DIRECT`.
7. [07-clickhouse-olap-optimization.md](05-dbms-optimization/07-clickhouse-olap-optimization.md) — Оптимизация ClickHouse: семейство `MergeTree`, разреженный индекс и гранулярность 8192, Data Skipping Indexes (Bloom filter), пакетная вставка, словари `dictGet` вместо тяжелых `JOIN`.

---

## 3. Соответствие стеку навыков Antigravity v17.5.0

Материалы репозитория спроектированы в строгом соответствии с набором из 108 ключевых навыков версии **v17.5.0**:

- **Data Engineering и Big Data**: `data-engineer`, `spark-optimization`, `airflow-dag-patterns`, `dbt-transformation-patterns`, `snowflake-development`, `data-engineering-data-pipeline`, `data-quality-frameworks`, `cc-skill-clickhouse-io`.
- **СУБД и SQL**: `sql-pro`, `sql-optimization-patterns`, `postgresql`, `postgres-best-practices`, `postgresql-optimization`, `database-design`, `database-architect`, `database-admin`, `database-optimizer`, `database-migration`, `vector-database-engineer`, `nosql-expert`, `drizzle-orm-expert`, `prisma-expert`.
- **Python и ML**: `python-pro`, `python-patterns`, `python-performance-optimization`, `python-testing-patterns`, `fastapi-pro`, `pydantic-models-py`, `pydantic-ai`, `polars`, `ml-engineer`, `ml-pipeline-workflow`, `scikit-learn`, `statsmodels`, `rag-engineer`, `langgraph`, `crewai`, `llm-app-patterns`, `llm-ops`.
- **Архитектура и отладка**: `software-architecture`, `senior-architect`, `clean-code`, `clean-code-guard`, `code-reviewer`, `systematic-debugging`, `debugger`, `test-driven-development`, `tdd-workflow`, `performance-profiling`.
- **DevOps и Linux**: `docker-expert`, `kubernetes-architect`, `linux-shell-scripting`, `bash-pro`, `git-advanced-workflows`, `github`, `gitlab-ci-patterns`, `terraform-skill`, `grafana-dashboards`, `prometheus-configuration`, `jq`, `tmux`.
- **Планирование и собеседования**: `planning-with-files`, `writing-plans`, `concise-planning`, `subagent-driven-development`, `interview-coach`.

---

## 4. Дорожная карта подготовки (Roadmap) на 6 недель

```text
[Недели 1-2: База] ──► [Недели 3-4: Пайплайны, DWH и Качество] ──► [Недели 5-6: Архитектура, Оптимизация и Оффер]
 - SQL Fundamentals       - Кимбалл и SCD 1-6                         - Spark Internals & AQE
 - ACID и Индексы         - Apache Spark & PySpark                    - NoSQL (Cassandra/DynamoDB)
 - Python & Генераторы    - dbt Transformations & Snowflake           - Vector DB & RAG Pipeline
 - ETL vs ELT             - Polars & Apache Arrow                     - Тюнинг СУБД (Postgres, ClickHouse, MPP)
                          - Great Expectations & Data Contracts       - System Design платформы
                          - Apache Airflow                            - STAR-самопрезентация и переговоры
```

1. **Недели 1–2 (Фундамент)**:
   - Полное освоение модулей из папки `01-junior`.
   - Решение 30 задач по SQL на LeetCode / StrataScratch (оконные функции, соединения, агрегации).
2. **Недели 3–4 (Стек, пайплайны и качество данных)**:
   - Глубокое изучение `02-middle` (dbt, Snowflake, Polars, Airflow, Data Quality).
   - Написание пайплайнов на PySpark/Polars и создание DAG в Airflow.
   - Закрепление понимания методологии Кимбалла, инкрементов в dbt и проверок Great Expectations.
3. **Недели 5–6 (Senior уровень, оптимизация и оффер)**:
   - Освоение модулей `03-senior` (NoSQL, Векторные БД, Lakehouse, Spark Internals) и `05-dbms-optimization`.
   - Решение кейсов по System Design и расчету распределенной нагрузки.
   - Повторение блиц-вопросов из `04-interview-cheatsheets`.
   - Подготовка самопрезентации по методу STAR с Earned Secrets и отработка переговоров по зарплате.
