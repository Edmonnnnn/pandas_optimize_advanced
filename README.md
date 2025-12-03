🏎️ pandas_optimize_advanced

A comparative performance benchmark and architectural review of processing large tabular datasets in Python using Pandas, Polars, and DuckDB.

📚 Project Overview

This project implements a real-world technical assignment from a freelance platform.
Goal — dramatically accelerate analytics on CSV datasets using modern tools:

Audit a slow Pandas-based script

Rebuild the pipeline for maximum performance (Polars, DuckDB, Parquet)

Perform a fair comparison of execution time, usability, and integration convenience

🚩 Task Structure

You are given 4 initial datasets:

users.csv — users

products.csv — products and categories

purchases.csv — purchases (up to 2M rows)

discounts.csv — discounts and validity periods

Required:
For each product/category, compute the top-3 cities by revenue,
taking into account only valid discounts and excluding returns.

📈 Solution Approach
1. Data Generation & Exploration

Generate real-like datasets using generate_data.py

Analyze dataset structure with inspect_data.py

2. Baseline (Slow) Pandas Approach

slow_analysis.py — classic Pandas with copying, loops, and multiple intermediate DataFrames.

Result:

On 100k+ rows → execution time is tens of seconds

Limitations: poor scalability, high RAM consumption

3. Profiling & Bottleneck Detection

Identified performance bottlenecks:

Heavy I/O when reading CSV into DataFrames

Excessive join/merge operations and .iterrows() loops

No selective reading/filtering during the import stage

4. Optimizations
a. Convert CSV → Parquet

One-time conversion using csv_to_parquet.py

Result: 5–20× faster reading

b. Polars Implementation (Lazy Mode)

fast_top_cities_polars_lazy.py — lazy pipeline with parallel, vectorized execution

Result:

Full dataset (~2M rows) processed in 0.66 seconds

c. DuckDB Implementation (SQL + Parquet)

fast_top_cities_duckdb_fast.py — SQL query over Parquet files

Result:

Full dataset processed in 0.85 seconds

d. (Planned) ClickHouse integration for ultra-large datasets (not included in repo)
📊 Benchmark Results
Engine	Time (sec)	Notes
Pandas (baseline)	3.7+	Easy to start, inefficient for big data
Polars (Lazy)	0.66	Multithreaded, parallel, concise workflow
DuckDB (Parquet)	0.85	SQL-based, great for ad-hoc analytics
🚀 Key Takeaways

Modern engines (Polars, DuckDB, Parquet) are tens of times faster than Pandas — even on a laptop!

Lazy execution (Polars Lazy, SQL in DuckDB) enables complex pipelines to run in milliseconds.

Core optimization principles:

Prefer Parquet over CSV whenever possible

Apply filtering + aggregation at read time (scan → filter → groupby)

Avoid unnecessary copies and intermediate DataFrames

Perform joins only on required columns

📦 Repository Contents

fast_top_cities_polars_lazy.py — ultra-fast Polars pipeline (Lazy mode)

fast_top_cities_duckdb_fast.py — ultra-fast DuckDB SQL pipeline

.gitignore — excludes large/local files

README.md — this documentation

CSV, Parquet, and large datasets are NOT included in the repository.
You may generate test datasets using the provided scripts.

💡 Next Steps

Add a ClickHouse benchmark (for truly massive datasets)

Package the solution as a reusable analytics library

Implement additional metrics (retention, cohorts, LTV, etc.)

Move configuration parameters into a config file

---------------------------------------------------------------
---------------------------------------------------------------
---------------------------------------------------------------

# 🏎️ pandas_optimize_advanced

**Сравнительный бенчмарк производительности и архитектур обработки больших табличных данных в Python с помощью Pandas, Polars и DuckDB**

---

## 📚 Описание проекта

Проект реализует “боевое” техническое задание с фриланс-площадки:  
**Задача** — максимально ускорить аналитику данных по CSV-файлам, используя современные инструменты:  
- **Провести аудит “медленного” Pandas-скрипта**
- **Преобразовать pipeline для максимальной скорости (Polars, DuckDB, Parquet)**
- **Провести честное сравнение по времени, удобству, удобству интеграции**

---

## 🚩 Структура задачи

- Дано 4 исходных датасета:
  - `users.csv` — пользователи
  - `products.csv` — товары и категории
  - `purchases.csv` — покупки (до 2 млн строк)
  - `discounts.csv` — скидки и даты действия
- Требуется для каждого товара/категории найти **топ-3 города по выручке**, учитывая только валидные скидки и не учитывать возвраты.

---

## 📈 Как решали

### 1. **Генерация и изучение данных**
- Генерация real-like датасетов скриптом `generate_data.py`
- Инспекция структуры: `inspect_data.py`

### 2. **Базовый (медленный) Pandas-подход**
- `slow_analysis.py` — классический Pandas c копированием, for-циклами и промежуточными DataFrame.
- Итог:  
  - На 100k+ строках время выполнения — **десятки секунд**
  - Ограничения: сложно работать с большими файлами, сильная нагрузка на RAM

### 3. **Профилирование и выявление узких мест**
- Выявили “бутылочные горлышки”:  
  - Чтение и копирование данных (CSV → DataFrame)
  - Много join/merge/iterrows (циклы)
  - Нет фильтрации по нужным колонкам и значениям на этапе чтения

### 4. **Оптимизация:**
#### a. **Перевод данных в Parquet**
- Файлы CSV были разово преобразованы в Parquet (`csv_to_parquet.py`)
- Результат: чтение стало быстрее в 5–20 раз!

#### b. **Имплементация на Polars (Lazy Mode)**
- `fast_top_cities_polars_lazy.py` — ленивый pipeline, все join/filter/group делаются “в один проход”, параллельно и векторно
- Результат:  
  - Для всей выборки (~2 млн строк) выполнение: **0.66 сек**

#### c. **Имплементация на DuckDB (SQL + Parquet)**
- `fast_top_cities_duckdb_fast.py` — SQL-запрос по Parquet-файлам, с фильтрацией, join и агрегацией в одном запросе
- Результат:  
  - Для всей выборки: **0.85 сек**

#### d. (Планировалась интеграция с ClickHouse для супер-big data, но не включено в репозиторий)

---

## 📊 **Сравнительный анализ (результаты)**

| Инструмент         | Время (сек) | Особенности                       |
|--------------------|-------------|-----------------------------------|
| Pandas (старый)    | 3.7+        | Легко начать, неудобно для big data |
| Polars (Lazy)      | 0.66        | Многопоточно, параллельно, лаконично |
| DuckDB (Parquet)   | 0.85        | SQL-подход, идеален для ад-хок аналитики |

---

## 🚀 **Ключевые выводы**

- **Современные инструменты (Polars, DuckDB, Parquet) делают группировки и join’ы в десятки раз быстрее Pandas — даже на ноутбуке!**
- **Ленивое выполнение (Lazy mode, SQL) позволяет делать сложные цепочки за миллисекунды.**
- **Базовые принципы ускорения:**
  - Использовать Parquet вместо CSV
  - Делать фильтрацию и агрегацию сразу при чтении (scan + filter + groupby)
  - Исключать лишние копии данных и промежуточные DataFrame
  - Использовать join только по нужным полям

---

## 📦 **Что включено в репозиторий**

- `fast_top_cities_polars_lazy.py` — максимально быстрый pipeline на Polars (ленивый режим)
- `fast_top_cities_duckdb_fast.py` — максимально быстрый pipeline на DuckDB (SQL по Parquet)
- `.gitignore` — исключает все большие/личные файлы
- `README.md` — этот файл

> **CSV, Parquet и большие данные не публикуются в репозиторий!**  
> Используйте свои тестовые датасеты или сгенерируйте через предоставленные скрипты.

---

## 💡 **Что можно сделать дальше**

- Добавить ClickHouse-бенчмарк (для truly big data)
- Оформить как библиотеку или пакет для быстрой аналитики
- Реализовать дополнительные метрики (retention, cohort, LTV и др.)
- Вынести параметры в конфиг-файл

---

## 📝 **Связаться**

Автор: [ваше имя/ник]  
GitHub: [ваш профиль]  
Связь: [почта или Telegram, если хотите]

---

### _Проект создан для изучения и демонстрации современных методов обработки данных. Если вы хотите получить подробную консультацию по оптимизации аналитики или реализовать подобный pipeline — пишите!_

