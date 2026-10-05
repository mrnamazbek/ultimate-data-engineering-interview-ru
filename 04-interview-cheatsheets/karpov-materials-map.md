# Как повторять материалы Karpov DE вместе с этим репозиторием

Исходные материалы находятся локально в `/Users/namazbekbekzhanov/Downloads/Karpov DE`: 63 PDF в девяти тематических папках. Названия ниже позволяют найти урок без включения исходных PDF в репозиторий. Страницы — номера страниц PDF, начиная с 1. Новые схемы и вопросы в репозитории написаны самостоятельно; это карта тем, а не копия курса.

| Папка курса | Что повторить | Где закрепить в репозитории |
|---|---|---|
| 1. Реляционные и МРР СУБД | Строковое/колоночное хранение, объекты БД, планы и распределение | [Основы СУБД](../01-junior/02-database-fundamentals.md), [Greenplum](../05-dbms-optimization/05-greenplum-mpp-optimization.md) |
| 2. Автоматизация ETL-процессов | DAG, сложные пайплайны, свои Operators/Hooks/Sensors | [Airflow](../02-middle/04-workflow-orchestration-airflow.md), [ООП](../01-junior/06-oop-for-data-engineers.md), [разработка ПО](../02-middle/13-software-engineering-for-data-engineers.md) |
| 3. Big Data | HDFS/YARN/Hive, HBase, Spark SQL, Kafka, профилирование | [Hadoop](../02-middle/12-hadoop-ecosystem-hdfs-yarn-hive.md), [Spark internals](../03-senior/01-distributed-systems-spark-internals.md), [streaming](../03-senior/03-streaming-realtime-architectures.md) |
| 4. Проектирование DWH | Зерно, нормализация, SCD, Data Vault, Anchor | [DWH](../02-middle/02-data-modeling-dwh.md), [Data Vault и Anchor](../02-middle/11-data-vault-and-anchor-modeling.md), [хеши](../02-middle/14-hashing-and-deduplication.md) |
| 5. Облачное хранилище | Облака, вычисления и хранение, DE в Kubernetes | [Cloud](../02-middle/05-cloud-data-engineering.md), [Kubernetes](../03-senior/10-kubernetes-for-data-engineers.md) |
| 6. Визуализация данных | Требования, Dashboard Canvas, подключение BI к витрине | [Вопросы 34–35 о метриках и потребителях](common-de-interview-questions.md), [моделирование фактов](../02-middle/02-data-modeling-dwh.md) |
| 7. Big ML | Распределённое обучение, SparkML, подготовка данных | [MLOps: признаки и корректность во времени](../03-senior/06-mlops-for-data-engineers.md) |
| 8. Управление моделями | Версионирование DVC, tracking и registry MLflow | [MLOps: DVC/MLflow](../03-senior/06-mlops-for-data-engineers.md) |
| 9. Управление данными | Роли, Data Security, Data Quality, Deequ | [Data Quality](../02-middle/09-data-quality-and-contracts.md), [governance и PyDeequ](../03-senior/09-data-governance-security-and-management.md) |

## Уроки, полезные для новых вопросов

- **«2. Автоматизация ETL-процессов / Урок 5. Разработка своих плагинов.pdf»**, стр. 3, 7–8: Operator, Hook и Sensor. Нарисуйте наследование и композицию, затем объясните, где выполняются I/O и бизнес-логика.
- **«4. Проектирование DWH / Урок 4. Методология Data Vault.pdf»**, стр. 16: тема хеширования. Дополнительно разберите канонизацию, риск коллизий и различие hash key/hashdiff по новому модулю.
- **«4. Проектирование DWH / Урок 5. Методология Anchor Modeling.pdf»**, стр. 3–6: Anchor, Attribute, Tie, Knot и цена множества JOIN. Нарисуйте сущность с двумя независимо историзируемыми атрибутами.
- **«9. Управление данными / 3 урок. Data Quality.pdf»**, стр. 5, 7: отчёт качества, хорошие/ошибочные данные и реакция на проверку. Сравните наблюдение за процессом с блокировкой публикации неверных данных.
- **«3. Big Data / 12 урок. Apache Kafkа. Spark streaming.pdf»**: topic/partition/offset и streaming. Детали гарантий и API сверяйте с официальной документацией выбранной версии.

## Способ повторения одного урока

```text
Урок → схема на листе → один вопрос → пример из 3 строк → сценарий сбоя → устный ответ
```

Пример: Hook/Operator → Operator использует Hook → «почему композиция?» → FakeHook возвращает две строки → timeout после записи → объяснить безопасный retry. Возвращайтесь к [памятке](interview-preparation-memo.md), чтобы выбрать следующий пробел.
