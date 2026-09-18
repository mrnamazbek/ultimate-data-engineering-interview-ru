# Python для инженера данных (Уровень Junior)

Python — главный язык разработки пайплайнов, ETL-скриптов и задач автоматизации в Data Engineering. На технических собеседованиях для Junior проверяют не абстрактные алгоритмы, а умение эффективно работать с памятью, понимать сложность операций со структурами данных и безопасно взаимодействовать с базами данных и файлами.

---

## 1. Встроенные структуры данных и сложность операций (Big-O)

Понимание того, как работают структуры данных внутри CPython, предотвращает деградацию производительности скриптов обработки данных.

| Структура | Внутреннее устройство | Доступ по индексу / ключу | Поиск элемента (`in`) | Добавление | Удаление |
|---|---|---|---|---|---|
| **List** (список) | Динамический массив указателей | $O(1)$ | $O(N)$ | $O(1)$ в конец, $O(N)$ в начало/середину | $O(N)$ |
| **Dict** (словарь) | Хэш-таблица с открытой адресацией | $O(1)$ в среднем | $O(1)$ по ключу | $O(1)$ в среднем | $O(1)$ |
| **Set** (множество) | Хэш-таблица (только ключи) | Не поддерживается | $O(1)$ в среднем | $O(1)$ в среднем | $O(1)$ |
| **Tuple** (кортеж) | Неизменяемый статический массив | $O(1)$ | $O(N)$ | Неизменяем | Неизменяем |

### Типичная ошибка на собеседовании
Проверка вхождения элементов в цикле по списку вместо множества:
```python
# ПЛОХО: сложность O(N * M)
valid_ids_list = [1, 2, 3, ...] # 100 000 элементов
for record in incoming_records: # 1 000 000 элементов
    if record["id"] in valid_ids_list: # O(N) на каждую итерацию!
        process(record)

# ХОРОШО: сложность O(M + N)
valid_ids_set = set(valid_ids_list) # O(N) разово
for record in incoming_records:
    if record["id"] in valid_ids_set: # O(1) поиск в хэш-таблице!
        process(record)
```

---

## 2. Потоковая обработка данных и генераторы (`yield`)

### Кратко: зачем нужны генераторы в Data Engineering
Если файл с данными весит 50 ГБ, а на сервере всего 8 ГБ оперативной памяти, вызов метода `f.readlines()` или загрузка файла целиком приведет к аварийной остановке скрипта из-за переполнения памяти (**OOM — Out Of Memory**). Генераторы позволяют обрабатывать данные лениво, строка за строкой или батчами.

### Реализация потокового генератора для чтения больших CSV-файлов
```python
import csv
from typing import Generator, Dict, Any

def stream_large_csv(file_path: str) -> Generator[Dict[str, Any], None, None]:
    """Лениво читает файл строка за строкой, расходуя фиксированный объем RAM (O(1) по памяти)."""
    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

# Использование генератора: память не переполняется даже на терабайтных файлах
total_revenue = 0.0
for record in stream_large_csv("big_transactions.csv"):
    total_revenue += float(record["amount"])
```

### Генератор батчей (пакетная обработка)
Для эффективной записи в базу данных требуется объединять строки в пакеты (пачки по 1000–5000 записей):
```python
from itertools import islice

def chunked_stream(iterable, chunk_size=1000):
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            break
        yield chunk
```

---

## 3. Коллекции стандартной библиотеки: `defaultdict` и `Counter`

Модуль `collections` ускоряет написание агрегаций без использования сторонних тяжелых библиотек.

```python
from collections import defaultdict, Counter

# 1. Counter: быстрый подсчет частотности событий
events = ["click", "view", "click", "purchase", "view", "click"]
event_counts = Counter(events)
# Результат: {'click': 3, 'view': 2, 'purchase': 1}
top_event, top_count = event_counts.most_common(1)[0]

# 2. defaultdict: группировка записей без проверок "if key in dict"
grouped_sales = defaultdict(list)
data = [("IT", 1200), ("HR", 800), ("IT", 1500)]
for dept, salary in data:
    grouped_sales[dept].append(salary)
```

---

## 4. Безопасная работа с СУБД (DB API 2.0 и защита от SQL-инъекций)

При написании скриптов выгрузки и загрузки данных необходимо строго следовать правилам параметризации запросов.

### Правило: никогда не форматировать SQL-строки через f-строки
```python
# КАТАСТРОФА: Уязвимость к SQL Injection
user_input = "10; DROP TABLE users; --"
query = f"SELECT * FROM users WHERE department_id = {user_input}"

# ПРАВИЛЬНО: Использование параметризованных запросов
# СУБД экранирует параметры самостоятельно и компилирует план запроса
import psycopg2

with psycopg2.connect("dbname=analytics user=de_user password=secret") as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM users WHERE department_id = %s AND is_active = %s;",
            (10, True)
        )
        records = cur.fetchall()
```

### Использование серверных курсоров (Server-side cursors)
По умолчанию клиентская библиотека (например, `psycopg2`) загружает весь результат запроса `cur.fetchall()` в память Python-процесса. При выборке миллионов записей это вызывает OOM. Для предотвращения этого создают именованный серверный курсор:

```python
# Именованный курсор заставляет PostgreSQL отдавать данные порциями
with conn.cursor(name="large_dataset_cursor") as named_cur:
    named_cur.itersize = 2000 # размер порции сетевого буфера
    named_cur.execute("SELECT * FROM fact_orders")
    for row in named_cur: # итерация без единовременной загрузки всех строк в RAM
        process_row(row)
```

---

## 5. Практические советы для собеседования
1. **Разница между `is` и `==`**: оператор `==` проверяет равенство значений объектов, тогда как `is` проверяет идентичность адресов в памяти (`id(a) == id(b)`). Одиночки, такие как `None`, всегда проверяются через `if x is None:`.
2. **Изменяемые аргументы по умолчанию**: никогда не используйте `def func(data=[])`. Список создается один раз при определении функции, и все последующие вызовы будут модифицировать один и тот же объект. Используйте `def func(data=None): if data is None: data = []`.
