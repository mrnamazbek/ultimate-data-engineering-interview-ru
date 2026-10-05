# 10. Kubernetes для Data Engineering и запуск распределенного Spark

## 1. Введение: Почему Kubernetes вытесняет традиционный Hadoop YARN

Исторически распределенные вычисления Big Data (Spark, MapReduce, Flink) запускались поверх менеджера ресурсов **Hadoop YARN**. 

Однако в корпоративной эксплуатации YARN имеет три критических недостатка:
1. **Конфликт зависимостей (Dependency Hell)**:
   Все задачи на кластере делят общую системную среду операционной системы. Если команде ML требуется Python 3.11 и PyTorch 2.0, а старым ETL-пайплайнам нужен Python 3.8 и специфичные версии C-библиотек, обновить кластер становится практически невозможно.
2. **Борьба за ресурсы и медленный скейлинг**:
   YARN рассчитан на статический пул серверов. Он не умеет динамически заказывать виртуальные машины в облаке (AWS/GCP/VK Cloud) под пиковую ночную нагрузку и выключать их утром.
3. **Разделение инфраструктуры**:
   Компании вынуждены были содержать два независимых кластера: Kubernetes для микросервисов и бэкенда, и отдельный Hadoop-кластер для дата-инженеров.

**Kubernetes (K8s)** объединил инфраструктуру: сегодня он стал единым стандартом развертывания как микросервисов, так и распределенных платформ данных (Spark, Airflow, Trino, Kafka, ClickHouse).

### Наглядно: контроллер поддерживает желаемое состояние

```text
Манифест: нужны 3 replicas → контроллер сравнивает с фактическим состоянием
Одна Pod потеряна → создаётся замена → Scheduler выбирает узел
```

Перезапуск процесса не гарантирует восстановление данных или корректность побочного эффекта. Kubernetes и YARN выбирают с учётом среды и эксплуатации.

---

## 2. Базовые концепции и абстракции Kubernetes для Data Engineer

```
                                  [ KUBERNETES CLUSTER ]
                                             │
      ┌──────────────────────────────────────┼──────────────────────────────────────┐
      ▼                                      ▼                                      ▼
[ Control Plane ]                     [ Worker Node 1 ]                      [ Worker Node 2 ]
├── API Server                        ├── kubelet                            ├── kubelet
├── etcd (хранилище состояния)        ├── containerd (CRI)                   ├── containerd (CRI)
├── Scheduler                         └── [ Pod: Spark Driver ]              └── [ Pod: Spark Executor ]
└── Controller Manager                     ├── ConfigMap (spark-conf)             ├── PVC (Scratch Space)
                                           └── Secret (S3 credentials)            └── CPU / RAM Limits
```

### 2.1. Вычислительные примитивы
- **Pod (Под)**: минимальная неделимая единица развертывания. Содержит один или несколько тесно связанных контейнеров, разделяющих сетевой стек (IP-адрес) и тома данных (Volumes).
- **Deployment**: контроллер для stateless-приложений (веб-интерфейс Airflow Webserver, аналитические API). Поддерживает плавное обновление версий (Rolling Update) без простоя.
- **StatefulSet**: контроллер для stateful-приложений, требующих сохранения состояния, стабильного сетевого имени и постоянного диска (Kafka, Zookeeper, MinIO, PostgreSQL).

### 2.2. Управление хранилищем (Storage)
В контейнерах файловая система по умолчанию временная (эфеемерная): при падении пода все файлы стираются.
Для постоянного хранения данных используются абстракции K8s:
- **PersistentVolume (PV)**: физический сетевой диск, выделенный в инфраструктуре (AWS EBS, Ceph RBD, NFS).
- **PersistentVolumeClaim (PVC)**: декларативная заявка пода на выделение дискового пространства определенного размера и типа.
- **StorageClass**: механизм динамического создания дисков (Dynamic Provisioning) через CSI-драйверы.

### 2.3. Конфигурация и безопасность
- **ConfigMap**: хранение неконфиденциальных параметров и конфигурационных файлов (`spark-defaults.conf`, `airflow.cfg`).
- **Secret**: хранение паролей, токенов и ключей доступа (к S3, базам данных) в закодированном виде Base64 с интеграцией с HashiCorp Vault.

### Наглядно: Pod, диск и секрет

```text
Pod → использует PVC → связанный PV; StorageClass задаёт provisioning
Deployment → заменяемые stateless replicas
StatefulSet → стабильные имена/связь с томами
ConfigMap → настройки; Secret → чувствительные значения с контролем доступа
```

StatefulSet не реализует протокол репликации БД автоматически. Base64 в Secret — кодирование; нужны подходящие RBAC и защита хранения. Данные в ephemeral storage не переживают удаление Pod.

---

## 3. Запуск Apache Spark на Kubernetes

Существует два основных архитектурных подхода к запуску Spark на K8s.

### 3.1. Native Spark-on-K8s (`spark-submit`)

Начиная с версии Spark 2.3, клиент Spark умеет напрямую общаться с API-сервером Kubernetes без сторонних прослоек:

```
[ Инженер: spark-submit ] 
            │
            ▼ 1. Запрос на создание Driver Pod
[ K8s API Server ] ──► [ K8s Scheduler ] ──► [ Запуск Driver Pod ]
                                                       │
                                 2. Driver запрашивает │
                                    создание воркеров  │
                                                       ▼
                             ┌─────────────────────────┴─────────────────────────┐
                             ▼                                                   ▼
                   [ Spark Executor Pod 1 ]                            [ Spark Executor Pod 2 ]
```

1. Клиент выполняет команду `spark-submit` с указанием мастера `k8s://https://<k8s-apiserver>:6443`.
2. Kubernetes создает **Driver Pod**.
3. Внутри Driver Pod запускается программа, которая обращается к K8s API и динамически заказывает необходимое количество **Executor Pods**.
4. Экзекьюторы выполняют вычисления и сбрасывают промежуточные данные Shuffle на локальные диски (через `emptyDir` тома).
5. По завершении расчета Executor Pods автоматически уничтожаются, освобождая ресурсы кластера.
6. Driver Pod переходит в статус `Completed`, сохраняя логи выполнения для просмотра через `kubectl logs` или Spark History Server.

**Пример команды запуска:**
```bash
spark-submit \
    --master k8s://https://10.96.0.1:443 \
    --deploy-mode cluster \
    --name user-analytics-etl \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=my-registry.company.com/spark:3.5.0-custom \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
    --conf spark.executor.memory=8g \
    --conf spark.executor.cores=4 \
    --conf spark.kubernetes.executor.request.cores=3800m \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio.storage.svc:9000 \
    local:///opt/spark/work-dir/app.jar
```

---

### 3.2. Spark-on-K8s Operator (Декларативный GitOps подход)

Для современных практик CI/CD и GitOps (ArgoCD) запуск через CLI `spark-submit` неудобен. 

Google Cloud разработал **Spark Operator**, который добавляет в Kubernetes пользовательские ресурсы (CRD): `SparkApplication` и `ScheduledSparkApplication`.

**Пример манифеста `spark-etl.yaml`:**
```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: daily-sales-agg
  namespace: data-platform
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "company-registry.com/data/spark-etl:v1.4.0"
  mainApplicationFile: "local:///app/daily_sales.py"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
  driver:
    cores: 2
    memory: "4g"
    labels:
      version: 3.5.0
    serviceAccount: spark-operator-sa
  executor:
    cores: 4
    instances: 10
    memory: "16g"
    labels:
      version: 3.5.0
```

Применение манифеста:
```bash
kubectl apply -f spark-etl.yaml
```
Оператор сам отслеживает жизненный цикл, перезапускает упавшие поды и собирает статус в состояние ресурса.

### Наглядно: native submit и operator

```text
Native: spark-submit → Driver Pod → запрос Executor Pods
Operator: SparkApplication → reconciliation → запуск Spark и управление состоянием
Оба пути: образ, service account, сеть, storage и resources
```

Requests влияют на scheduling; CPU limit может ограничивать CPU, memory limit — приводить к OOMKill. Образ и конфигурация должны быть воспроизводимы; Operator не устраняет необходимость понимать Spark.

---

## 4. Apache Airflow на Kubernetes: Celery vs KubernetesExecutor

| Параметр | CeleryExecutor | KubernetesExecutor |
| :--- | :--- | :--- |
| **Принцип работы** | Статический пул постоянных воркеров Celery слушает очередь Redis/RabbitMQ | На каждый TaskInstance динамически создается отдельный изолированный K8s Pod |
| **Изоляция сред** | Все DAG делят общие установленные Python-библиотеки на воркере | Каждый шаг может запускаться в **своем собственном Docker-образе** |
| **Использование ресурсов** | Воркеры работают постоянно, расходуя CPU/RAM даже при простое | Ресурсы выделяются строго на время работы таски и сразу освобождаются |
| **Накладные расходы старта** | Мгновенно (< 100 мс) | 2–10 секунд на создание K8s пода и скачивание образа |
| **Идеально для** | Тысяч быстрых коротких SQL-запросов и сенсоров | Тяжелых разнородных ETL/ML вычислений с изоляцией сред |

### Использование `KubernetesPodOperator` в Airflow
Позволяет инженеру данных запустить задачу, написанную на любом языке (C++, Go, Rust, R), упакованную в Docker:

```python
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG("k8s_container_pipeline", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
    
    heavy_ml_task = KubernetesPodOperator(
        task_id="run_custom_model",
        name="custom-model-runner",
        namespace="data-platform",
        image="my-repo/ml-engine:v2.1",
        cmds=["python", "train_and_predict.py"],
        arguments=["--dataset-path", "s3://lake/gold/"],
        resources={
            "request_cpu": "4",
            "request_memory": "16Gi",
            "limit_cpu": "8",
            "limit_memory": "32Gi"
        },
        is_delete_operator_pod=True, # Удалять под после успешного завершения
        get_logs=True
    )
```

### Наглядно: executor Airflow и отдельный Pod задачи

```text
CeleryExecutor → worker запускает задачу
KubernetesExecutor → задача исполняется в task Pod
KubernetesPodOperator → сама задача создаёт отдельный workload Pod
```

KubernetesPodOperator можно использовать с разными executors. Не путайте инфраструктуру исполнения task с Pod, который эта task запускает для внешней работы.

---

## 5. Вопросы с Senior собеседований

### Вопрос 1: Как в Spark на Kubernetes решается проблема потери промежуточных данных Shuffle при удалении Executor Pod?
**Ответ**:
В YARN существовал внешний сервис `External Shuffle Service` (ESS), сохранявший shuffle-файлы на ноде. В Kubernetes традиционный ESS сложен в поддержке.
Современные решения в Spark 3.x:
1. **Shuffle Tracking (Dynamic Allocation)**: Spark Driver отслеживает, на каких экзекьюторах остались shuffle-файлы, и откладывает их удаление до тех пор, пока данные не будут прочитаны редьюсерами.
2. **Push-Based Shuffle (Magnet)**: экзекьюторы сливают блоки shuffle на выделенные серверы слияния.
3. **Поддерживаемые механизмы сохранения/восстановления shuffle**: PVC-based recovery, decommissioning или подходящий remote shuffle plugin — если их поддерживает конкретная версия и конфигурация. Простая подстановка `s3://...` в `spark.local.dir` не превращает локальный shuffle в объектный storage: эта настройка ожидает доступные файловые пути.

Shuffle tracking помогает при плановом удалении executor, но не предотвращает аварийную потерю Pod/диска. Потерянные блоки могут требовать повторного выполнения upstream tasks по lineage. Возможность push-based shuffle и внешний сервис слияния проверяют для выбранного cluster manager.

### Вопрос 2: В чем разница между `requests` и `limits` ресурсов в манифесте K8s для Data Engineering задач?
**Ответ**:
- **`requests`**: заявленная потребность, используемая scheduler при выборе узла и распределении ресурсов. Если Pod не помещается в доступную allocatable capacity по requests, он может остаться Pending. Это не универсальная гарантия эксклюзивного CPU или реального размера рабочего набора памяти.
- **`limits`**: ограничение потребления. CPU limit может приводить к throttling, memory limit — к OOM kill при попытке превысить доступную память. Exit code 137 сам по себе не доказывает OOM: проверьте reason и события. Для Spark учитывайте JVM heap, native memory, Python workers и overhead.
