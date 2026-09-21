# Taldau ELT: инвестиции в основной капитал

Локальная конфигурация: скопируйте `.env.example` в `.env` (не перезаписывайте существующий
`.env`) и заполните локальные secret values. `.env` и его варианты исключены из Git;
`.env.example` содержит только placeholders. Compose автоматически читает `.env` и требует
обязательные переменные. URI Airflow metadata DB задаётся целиком через
`AIRFLOW_METADATA_SQL_ALCHEMY_CONN`; credentials в URI должны быть URL-encoded.

CLI и read-only source fixture тестов используют `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`,
`PGPASSWORD` из окружения процесса. Экспортируйте их в сессию перед запуском Python:
Python сам `.env` не читает. Если `PGPASSWORD` отсутствует, libpq может использовать `.pgpass`
(`pgpass.conf` на Windows); встроенного пароля по умолчанию нет. При отсутствии подходящих
credentials libpq выдаёт ошибку подключения. Airflow DAG продолжает использовать Connection
`digest_target_db`. Пароль временного test PostgreSQL генерируется отдельно при каждом запуске,
передаётся контейнеру и тестам через environment и не выводится в лог.

Подготовленная годовая загрузка описана в [README_SNAPSHOT_2025.md](README_SNAPSHOT_2025.md).
Её код добавлен отдельным DAG; полный extraction ожидает подтверждения.

Локальный проверенный пилот: **701827 / period_id=8 / Астана 268012 / декабрь 2025 / reporting_period=1069**.
Полная загрузка Казахстана не включена. Основной DAG `taldau_pipeline` и его таблицы работают отдельно.

## Что было и что изменено

В старом DAG: `extract -> transform (Python wide-to-long) -> load_silver -> build_gold (SQL)`.
Его `taldau.metadata_taldau_indicators` содержит в том числе 701827 с period_id=7 и другими измерениями;
это другой набор данных, его конфигурация сохранена.

Новый DAG `taldau_inv_fixed_assets`:

```text
get_metadata -> extract_load_bronze -> validate_bronze
  -> transform_silver_sql -> validate_silver -> build_gold_sql -> validate_gold
```

Python выполняет HTTP, запись полного ответа, обход по `id`/`leaf`, retries, проверку структуры и orchestration.
Python не разбирает значения фактов, не удаляет `x`, не выбирает период до записи Bronze.
SQL PostgreSQL выполняет разбор JSONB, фильтрацию `x`, проверку grain, приведение NUMERIC и дат,
загрузку справочников, Silver и Gold.

Чтобы позже перевести region_metric на ELT, нужно отдельно заменить его `extract` на Bronze loader,
а `transform/load_silver` на SQL с прежним grain. Существующий SQL Gold можно вынести в миграции,
сохранив `taldau.gold_v_region_year_metrics`. Cube не направляется в `taldau.silver_statistics_region`.

## Файлы

| Файл | Назначение |
|---|---|
| `dags/taldau_elt/loader.py` | HTTP -> raw Bronze, checkpoint/resume, ограничение пилота |
| `dags/taldau_elt/sql/001_bronze.sql` | Bronze, журнал запусков, отдельные metadata с pipeline_type |
| `dags/taldau_elt/sql/002_silver.sql` | SQL views, проверки, точный NUMERIC, Silver, территории |
| `dags/taldau_elt/sql/003_gold.sql` | Dimensions, fact, mart, проверка Silver/Gold |
| `dags/taldau_elt/pipeline.py` | Тонкие Python-обёртки SQL |
| `dags/taldau_inv_fixed_assets.py` | Отдельный DAG без расписания, один активный запуск |
| `tools/run_investments_elt.py` | CLI для миграций и пилота |
| `tools/verify_investments_pilot.py` | Сверка с локальным контрольным CSV |
| `tests/test_investments_elt.py` | Unit и транзакционные интеграционные проверки |
| `data/reports/astana_elt_validation.json` | Результаты реальной загрузки и сверки |
| `data/reports/astana_elt_execution.json` | Состояния задач Airflow и сохранность старого pipeline |

`docker-compose.yaml`, credentials, порты, существующие скрипты и старый DAG не изменены.
Новые файлы доступны в контейнерах через существующий mount `./dags:/opt/airflow/dags`.

## Bronze и версии

`taldau.bronze_taldau_api_raw`: один полный ответ на запрос. Сохраняются endpoint, все параметры,
indicator_id, period_id, HTTP status, время, run_id, контекст обхода, JSONB и исходный текст HTTP body.
API возвращает много периодов сразу; они остаются в Bronze, даже если Silver пилота берёт только декабрь.
В запросе KATO root также возвращаются соседние территории. Это неизменённый ответ для определения
иерархии; их кубы не обходятся и факты других территорий не публикуются.

JSONB сохраняет значения и типы, включая строки `"x"` и `"1798175695000"`.
JSONB нормализует форматирование/порядок ключей, поэтому дополнительно хранится `response_text`.
Текст отправляется в PostgreSQL как `%s::jsonb` напрямую, без сериализации чисел через Python float.
Сохраняется декодированное тело ответа, не сетевые байты сжатого HTTP.

* `request_hash = SHA256(endpoint + canonical exact string request parameters)`.
* `UNIQUE(run_id, request_hash)` предотвращает дубли внутри extraction run.
* Одинаковый run_id продолжает прерванный обход по уже сохранённым ответам.
* Успешно законченный run_id не скачивает API повторно.
* Новый run_id сохраняет новую версию ответов, включая пересмотренную историю.
* `response_hash` фиксирует содержимое исходного текста. Одинаковые ответы разных запусков намеренно
  остаются отдельными наблюдениями; бесконтрольного размножения при retry нет.
* Конфигурация и scope фиксируются в `taldau.bronze_extraction_runs`; несовместимое переиспользование run_id запрещено.
* Каждый HTTP response сохраняется одной транзакцией вместе со всеми его узлами. Уже сохранённые ответы
  переживают ошибку/перезапуск; run становится complete только после всего обхода.
* Session lock запрещает одновременный обход одного run_id; API pool имеет 3 слота, task использует 1,
  а внутри выполняется только один запрос одновременно. Между запросами пауза 0.2 с.
* Retry: до 5 повторов для 429/500/502/503/504, backoff, Retry-After, connect/read timeout 10/90 с.
  После исчерпания повторов DAG завершается ошибкой, сохраняя checkpoint. HTTP ошибки логируются;
  non-JSON error pages не выдаются за raw JSON.

Длительный обход не является атомарным снимком API: источник может измениться между запросами.
Для сверки сохраняются времена каждого ответа и границы запуска.

## Silver и Gold

`taldau.silver_inv_fixed_assets` имеет ключ
`(indicator_id, reporting_period, kato_id, krp_id, sif_id, gsvziok_id)`.
Уникальность подтверждена на пилоте; для страны потребуется отдельная проверка.
Строка хранит source_run_id, source_raw_id, порядковый номер исходного узла.

Контрольный диапазон пилота: 306000 .. 2492557598000; все значения целые.
`NUMERIC` без precision/scale выбран для точности и сохранения возможных дробей без округления.
Неизвестные нечисловые значения останавливают проверку, `x` исключается только SQL-преобразованием.
Отсутствующие пары период/комбинация не создаются. Intermediate nodes сохраняются.

Для period_id=8 даты означают накопительный период: start_date = 1 января,
end_date и period_date = последний день месяца. Это правило нельзя автоматически применять к другим типам периода.

`taldau.silver_dim_territory` использует source ID; parent_id и depth берутся из фактических запросов дерева.
`territory_level` пока NULL: depth не выдаётся за административный уровень.
В пилоте собраны корень и непосредственные дети; полный справочник населённых пунктов ещё не загружен.

После проверки полного среза SQL атомарно заменяет только его строки. Это обрабатывает в том числе
исчезновение комбинаций и numeric -> x. Не прошедший validation срез не удаляет прежние данные.
Старый run не может перезаписать опубликованный более новый срез.

Gold использует `dim_inv_member`, `dim_inv_period`, `fact_inv_fixed_assets`, `v_inv_fixed_assets`.
`mart_inv_territory_totals` выбирает источник для KRP/SIF/GSVZIOK = Всего без суммирования.
В facts присутствуют и родители, и дети; без явного выбора уровня суммировать их нельзя.
Значения месячные с накоплением: суммы по месяцам также некорректны.

## Запуск из PowerShell

Из корня проекта; CLI использует прежние локальные credentials и стандартные переменные PGHOST,
PGPORT, PGDATABASE, PGUSER, PGPASSWORD для переопределения. Airflow использует `digest_target_db`.

```powershell
.venv/Scripts/python.exe tools/run_investments_elt.py migrate-bronze
.venv/Scripts/python.exe tools/run_investments_elt.py extract --run-id astana-2025-12-pilot-v1
.venv/Scripts/python.exe tools/run_investments_elt.py migrate-silver
.venv/Scripts/python.exe tools/run_investments_elt.py transform --run-id astana-2025-12-pilot-v1
.venv/Scripts/python.exe tools/run_investments_elt.py validate --run-id astana-2025-12-pilot-v1
.venv/Scripts/python.exe tools/run_investments_elt.py gold --run-id astana-2025-12-pilot-v1
.venv/Scripts/python.exe tools/verify_investments_pilot.py
```

Миграции идемпотентны и выполняются в транзакциях. `migrate-silver` создаёт также пустые структуры Gold;
их заполнение происходит отдельным этапом после проверки Silver.
Для перерасчёта без интернета выполняйте `transform`, `validate`, `gold` с уже загруженным run_id.
Для новой версии источника используйте новый run_id. Если источник изменился и не даёт 504/7,
пилот остановится для расследования; ожидания нельзя менять ради зелёного запуска.

В Airflow UI http://localhost:8082 откройте `taldau_inv_fixed_assets` и задайте при запуске:

```json
{"bronze_run_id":"astana-2025-12-pilot-v1"}
```

Это replay сохранённого Bronze. При null создаётся новая загрузка Астаны с run_id текущего DAG run.
Проверенный scheduler run: `elt_pilot_offline_validation`, все семь задач завершились `success`.
При первой проверке исправлен конфликт аргумента `run_id` с контекстом Airflow; повторная попытка
успешна. В задачах используется имя `source_run_id`.
У DAG нет расписания, max_active_runs=1, pool=taldau_api. Первое обнаружение нового файла может
занять до 300 секунд согласно текущему refresh_interval.

## Проверка

```sql
SELECT taldau.bronze_validate_inv_pilot('astana-2025-12-pilot-v1');
SELECT count(*) FROM taldau.silver_inv_fixed_assets WHERE kato_id=268012 AND reporting_period=1069;
SELECT raw_value,count(*) FROM taldau.bronze_v_inv_candidates
WHERE run_id='astana-2025-12-pilot-v1' AND value IS NULL GROUP BY raw_value;
SELECT * FROM taldau.gold_mart_inv_territory_totals;
```

Результат пилота: 414 raw responses, 511 комбинаций, 504 numeric, 7 x, дублей 0.
CSV-сверка: matched 504, missing 0, extra 0, value mismatch 0.
Контрольная строка KRP=741927/SIF=807855/GSVZIOK=19202537: 1798175695000.
Локальная `public.bns_inv_fixed_assets` отсутствует. Выполнена сверка с существующим
`data/astana_2025_12_taldau.csv`, ранее подтверждённым пользователем против ETS;
это не новая прямая сверка с ETS. SHA256 файла записан в отчёте.

```powershell
$env:TALDAU_TEST_DB='1'
.venv/Scripts/python.exe -m unittest discover -s tests -v
```

Интеграционные тесты используют копии пилотного Bronze в одной транзакции и откатывают все изменения.
Проверяют resume без HTTP, дубли, unknown values, x, неполный run и точный decimal при SQL rebuild.
До миграций создан backup в `data/backups/20260917_203152/taldau_before_elt.dump`.
Volumes не удалять; команду `docker compose down -v` не использовать.

## Следующий этап: весь 2025, без запуска сейчас

1. Добавить журнал заданий extraction по территории с queued/running/complete/failed и lease/attempt.
   Зафиксировать source KATO hierarchy; не предполагать административные уровни по глубине дерева.
2. Разделить общий snapshot_id и территориальный chunk_id. Хеш кэша — точные параметры запроса
   в пределах snapshot. Разные территории/КРП/СИФ нельзя считать одинаковым запросом.
3. Расширить scope до списка/диапазона периодов; SQL развернёт только реально присутствующие
   пары ключей `yMMYYYY`/`MMYYYY`. Текущие pilot функции намеренно принимают один срез.
4. Выдавать по одному территориальному chunk на mapped task, pool не более 3; согласовать checkpoint
   и batch insert перед публикацией. Не загружать весь payload в XCom или общий Python list.
5. Сделать кандидатный SQL-слой для всего 2025 и проверить natural key до публикации.
   Проверять конфликтующие определения hierarchy между контекстами; при изменениях нужен
   versioned справочник, а не произвольный выбор последнего имени/родителя.
6. Подключить реальный ETS snapshot. Сначала проверить типы/семантику kato1/krp/sif/gsvziok и
   подготовить подтверждённое соответствие source IDs. space_element_set_id не изобретать.
   Сверять полный grain через FULL OUTER JOIN: missing/extra/value mismatch/duplicates, отдельно
   по каждому месяцу. Имена без проверенного ID mapping не являются надёжным ключом.
7. Ожидания ETS 2025 ниже служат диагностикой. Современные исправления Taldau могут давать отличия;
   расследовать их по raw response и времени загрузки, не дописывать/обрезать строки ради count.
8. Публиковать Silver только для полностью загруженных и проверенных срезов, затем Gold.
   Сохранять предыдущую публикацию при ошибке. Для принятого ETS migration gate хранить результат
   reconciliation отдельно от статуса извлечения.

| reporting_period | Месяц 2025 | ETS rows |
|---|---|---:|
|1071|Январь|22985|
|1076|Февраль|33174|
|1077|Март|41240|
|1078|Апрель|48565|
|1079|Май|53019|
|1080|Июнь|58159|
|1075|Июль|60380|
|1074|Август|62610|
|1073|Сентябрь|65150|
|1072|Октябрь|66747|
|1070|Ноябрь|68181|
|1069|Декабрь|71155|
|Всего||651365|

## Предлагаемая incremental стратегия

После подтверждения полной загрузки: каждый плановый snapshot повторно запрашивает текущий год
и предыдущий год, новые периоды добавляются автоматически по metadata источника. Ежемесячно
перепроверять более старую историю, разбивая территории на ограниченные задания; периодичность
согласовать с реальным временем обхода и частотой публикаций.

Сейчас API отдаёт все периоды за один dimension request: фильтр Silver по году сам по себе
не уменьшит число запросов. Если надёжный фильтр API/changed-since не найден, выполнять повторный
обход с новым snapshot_id и сравнивать содержимое, сохраняя новые версии. Нельзя использовать
старый cache бесконечно или считать наличие периода признаком неизменности.

Удалять исчезнувшие факты только при подтверждённой полноте соответствующего среза. Отдельно
отчитывать пересмотр значения, numeric -> x, появление и исчезновение комбинаций.

## Документация библиотек

* PostgreSQL JSON/JSONB: https://www.postgresql.org/docs/17/datatype-json.html
* Точный NUMERIC: https://www.postgresql.org/docs/17/datatype-numeric.html
* HTTP Retry: https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html
