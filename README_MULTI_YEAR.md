# Investments snapshots: 2023–2026

Только indicator 701827 / period_id 8. Транспорт, retry/backoff, grain, pool, waves,
checkpoints и ручной publication gate сохранены. Старые DAG ID и filenames с `2025`
сохранены для совместимости с Airflow history и CLI. Они больше не ограничивают scope одним годом.

```
read-only audit / plan
        ↓
prepare: frozen scope + ссылки на выбранную версию Bronze
        ↓
reuse (строго offline) ИЛИ отдельно разрешённый launch (только cache misses)
        ↓
stage → validate → diagnostics → STOP
        ↓ отдельное подтверждение
manual publish → Silver → Gold, одна транзакция
```

## Аудит привязок к 2025

| Место до изменения | Решение |
|---|---|
| `snapshots.py:create_snapshot`, проверка year=2025, year в run scope | Добавлены keyword arguments year_start/year_end/reuse_snapshot_id; legacy year/default 2025 сохранён |
| `004_snapshot_model.sql`: CHECK(year=2025), stage key regex | Не переписан; миграция 008 снимает старый CHECK и заменяет stage-функцию диапазоном |
| `005_snapshot_validation.sql`: regex года, expectations, даты, year DELETE, 651365 в результате | Не переписан; 008 переопределяет функции, expectations выбираются по диапазону, delta только для известных baseline |
| `007_gold_snapshot_publish.sql`: проверки и DELETE одного года | Не переписан; 008 обобщает indicator/year_start..year_end |
| `006_ets_reconciliation.sql`: capture только 2025, сравнение всего Silver snapshot | Не переписан; 008 разрешает baseline за 2023–2026, сравнивает выбранный год ETS с тем же годом Direct |
| `reports.py`: regex, JOIN expectations USING(year), сумма ETS одного года | Диапазон, nullable expectations/delta, отдельная observed year coverage |
| `taldau_inv_fixed_assets_2025.py`: authorize year=2025, описание/tag | Проверка диапазона; ID/filename и self-trigger сохранены; STOP после diagnostics сохранён |
| CLI `launch`: фиксированный DAG ID, default snapshot 2025 | DAG ID сохранён; существующий snapshot читается без сброса scope; prepare принимает диапазон |
| `loader.py:PILOT_SCOPE`, `002_silver.sql` pilot guard, pilot DAG/CLI run ID | Намеренно сохранены: bounded pilot December 2025 |
| `tools/count_astana_cube.py`, `extract_inv_fixed_assets_2025.py`, `verify_investments_pilot.py` | Старые исследовательские/pilot utilities сохранены, не используются новым workflow и не запускались |
| `tests/test_investments_elt.py`, `test_investments_snapshots.py`, runner fixture | Старые 2025 regression cases сохранены; добавлен отдельный multi-year suite |
| ETS expectations 2025 в 004, старые CSV/JSON/README имена | Это версия baseline/история, не runtime scope; данные не переписаны |

Source IDs `19202525`, `19202537` содержат цифры 2025, но не являются ограничениями по году.

## Модель и provenance

008 добавляет nullable `year_start`, `year_end`, `reuse_snapshot_id` к `bronze.inv_snapshots`.
Существующие строки не backfill-ятся: эффективные границы — `coalesce(year_start,year)` и
`coalesce(year_end,year)`. Для новых snapshots `year=year_start`; диапазон ограничен 2023–2026.
Indicator и endpoint остаются в frozen config. Scope и выбранный source нельзя поменять повторным prepare.

При `prepare --reuse-snapshot-id SOURCE` source должен быть validated/published с точно тем же config.
`bronze.inv_reuse_raw(snapshot_id,request_hash,raw_id)` фиксирует ссылки на эту версию.
Не выбирается случайный «самый новый» ответ из всей истории. Конфликтующие версии одного запроса
в source блокируют prepare. Raw JSON, response_text и исходный run_id не копируются и не изменяются.
Constraint `UNIQUE(run_id,request_hash)` остаётся на месте.

Loader сначала проверяет свой checkpoint, затем собственный raw, затем frozen reuse reference.
Проверяются hash, endpoint, canonical params, indicator, period type, dimension и tree_depth.
Успешный checkpoint нового run ссылается на прежний raw_id. `bronze.inv_run_raw` даёт логический
run_id нового обхода и отдельный `source_run_id` физического raw. Staging/coverage читают это view,
а Silver/Gold сохраняют исходный `source_raw_id`. Любой факт прослеживается до original response.
Новый snapshot получает собственные discovery inventory, chunks, leases и checkpoints.

Для новой версии источника создавайте новый snapshot без `--reuse-snapshot-id` и разрешайте HTTP
отдельно. Reuse старого snapshot восстанавливает его наблюдения, не обновляет статистику.
Если разрешить заполнение недостающих запросов поверх старого cache, получится версия с разными
датами наблюдения: raw.loaded_at и audit timestamps позволяют это видеть.

## Coverage и partial year

`audit` считает только реально встреченные GSVZIOK period/value keys; parent nodes и x сохранены.
Reporting-period IDs извлекаются из JSON, не из ETS expectation. Audit показывает raw observations,
а не обещает unique/validated facts: это проверяется после staging. Orphan/invalid считаются отдельно.

`plan` воспроизводит дерево по сохранённым id/leaf и точным request params. Пропущенный ответ
становится missing frontier, HTTP не вызывается. При missing>0 число дополнительных запросов
неизвестно: ответ может открыть новые ветви. `expected_http_requests=null`, нижняя граница находится
в `expected_http_requests_lower_bound`. При полном cache число точно равно нулю.
Структурные поля дерева обрабатываются в памяти CLI; большие ответы/списки не идут через XCom.

Один traversal возвращает несколько лет, поэтому request completeness общая для всех лет диапазона.
Months coverage показывается отдельно по каждому году. Отсутствие месяца в response не делает
HTTP-запрос missing: повторить тот же запрос для получения более свежей версии — отдельное решение.

`quality.inv_year_coverage` хранит логику observed coverage в view над сохранённым staging:
year, period_codes, months_present, available_through, numeric_rows, x_rows, warning.
Dashboard/API может прочитать её по опубликованному snapshot. Полный список кодов позволяет
отличить январь–август от ряда с пропусками; один available_through этого не доказывает.

Исторический год с <12 месяцами получает **explicit warning**, не автоматический blocking check:
источник может не содержать месяцы при полном обходе. Полностью пустой snapshot, неполный traversal,
orphan pairs, invalid values, конфликты, NULL и duplicate keys по-прежнему блокируют публикацию.
Решение о допустимости отсутствующих исторических месяцев принимает пользователь перед manual publish.
2026 не требует 12 месяцев. Отсутствующие факты/месяцы не генерируются.
Diagnostic rows из ETS expectations могут иметь `present_in_snapshot=false`; это не факты.
Общий delta суммирует только месяцы с ETS expectation, а не сравнивает весь multi-year объём с 2025.

## Публикация и ограничения

Публикация заменяет только indicator snapshot и годы внутри его диапазона, включая исчезнувшие
комбинации. Silver и Gold выполняются в одной транзакции; counts и двунаправленные EXCEPT сохранены.
Другие indicators и годы вне диапазона не удаляются. Повторная публикация идемпотентна по фактам.
Защита от старой версии сохраняет проверку времени создания и учитывает возраст reused raw:
новое имя snapshot не делает старые наблюдения свежими. Совпадающие source_raw_id разрешают
переиздание той же версии в расширенном scope. При неоднозначной свежести проверка консервативна.

Изменение period_code/start_date/end_date для уже существующего Gold reporting_period блокируется:
source period ID не может незаметно перенести чужие факты в год заменяемого scope.

Справочники Gold остаются текущими (SCD1), как в существующем проекте: names/parents общего source ID
могут обновиться для всех лет. Исторические версии иерархий отдельно не реализованы.
Источник не даёт атомарного снимка API: время наблюдения каждого raw сохраняется.
Validation ещё не доказывает полноту исторической географии: reuse использует inventory source snapshot.
Если в 2023 были территории, отсутствующие в discovery этой версии, кэш этого не обнаружит.

## Команды локально

Read-only аудит существующего 2025 snapshot работает **до миграции 008**:

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py audit --source-snapshot-id kz-investments-2025-v1 --year-start 2023 --year-end 2026 --output data/reports/bronze_2023_2026_audit.json
.venv/Scripts/python.exe tools/manage_investments_snapshot.py plan --source-snapshot-id kz-investments-2025-v1 --year-start 2023 --year-end 2026 --output data/reports/bronze_2023_2026_plan.json
```

Дальнейшие команды подготовлены, но к рабочей БД в этой задаче не применяются:

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py migrate
.venv/Scripts/python.exe tools/manage_investments_snapshot.py prepare --snapshot-id kz-investments-2023-2026-v1 --year-start 2023 --year-end 2026 --reuse-snapshot-id kz-investments-2025-v1
.venv/Scripts/python.exe tools/manage_investments_snapshot.py coverage --snapshot-id kz-investments-2023-2026-v1 --output data/reports/multi_year_coverage.json
.venv/Scripts/python.exe tools/manage_investments_snapshot.py reuse --snapshot-id kz-investments-2023-2026-v1
.venv/Scripts/python.exe tools/manage_investments_snapshot.py validate --snapshot-id kz-investments-2023-2026-v1
.venv/Scripts/python.exe tools/manage_investments_snapshot.py summary --snapshot-id kz-investments-2023-2026-v1
.venv/Scripts/python.exe tools/manage_investments_snapshot.py diagnostics --snapshot-id kz-investments-2023-2026-v1 --output data/reports/multi_year_diagnostics.json
```

`reuse` восстанавливает полный доступный traversal/staging offline, но не валидирует/публикует
автоматически. Если plan неполон, возвращает missing plan без загрузки. При падении полного
offline восстановления повторный reuse пропускает complete chunks и использует прежние checkpoints.

Только если plan показывает missing и пользователь отдельно разрешил сеть:

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py launch --snapshot-id kz-investments-2023-2026-v1 --confirm-full-extraction
```

Только после просмотра validation/diagnostics и отдельного подтверждения:

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py --snapshot-id kz-investments-2023-2026-v1 publish
```

CLI reply публикации остаётся `{"published_rows": N}`. Годовой DAG по-прежнему заканчивается
diagnostics, даже при успешной загрузке. Уже validated/published snapshot не запускается повторно.

## Готовность к серверу (deploy не выполнялся)

* Перенести `dags/taldau_elt/*.py`, все `dags/taldau_elt/sql/001...008*.sql`,
  `dags/taldau_inv_fixed_assets_2025.py`, `tools/manage_investments_snapshot.py`,
  `tools/run_investments_elt.py`. Сохранить относительную структуру dags/tools/sql.
  Pilot `dags/taldau_inv_fixed_assets.py` и legacy `dags/taldau_pipeline.py` переносить без изменений,
  если они нужны на сервере. В tests/runtime SQL нет зависимостей от локального Windows пути.
* Проверенная локальная база — PostgreSQL 17. Airflow DAG использует SDK 3.3.1 и
  `apache-airflow-providers-standard` (TriggerDagRunOperator). Runtime пакеты:
  `requests`, `urllib3`, `psycopg2`/`psycopg2-binary`, `pendulum`; Python с синтаксисом 3.10+.
  На сервере проверить совместимость с установленной версией Airflow, не обновлять её вслепую.
* Airflow Connection ID `taldau_dwh`: host, port, database/schema, login и secret из серверного
  secret store. Адрес БД задаётся относительно worker. Connection не должен указывать на Airflow metadata DB.
  Пароли и connection URI в документацию/репозиторий не переносить.
* Pool `taldau_api` — 3 slots. Mapped tasks: 1 slot, максимум 3 tasks, 1 HTTP за раз;
  waves <=128 IDs, max_active_runs=1. Настройки retry/backoff сохранены.
  Никакие Airflow Variables для этого workflow не нужны.
* CLI использует стандартные `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`;
  на сервере задавать их через окружение/secret mechanism, не полагаться на локальные defaults.
  `TALDAU_AIRFLOW_COMMAND=airflow` переключает launch с локального Docker Compose на серверный CLI.
  Значение — доверенная команда администратора, shell не используется.
* `TALDAU_REPORT_DIR` — каталог для DAG reports, default `/opt/airflow/data/reports`.
  Workers должны иметь права записи и общий persistent storage при распределённом executor.
  Локально используется существующий `data` volume. CLI `--output` задаёт путь отдельно.
* На чистой DWH применить `python tools/run_investments_elt.py migrate-bronze`,
  затем `python tools/run_investments_elt.py migrate-silver`,
  затем `python tools/manage_investments_snapshot.py migrate` (004–008 одной транзакцией).
  На текущей схеме достаточно последней команды. Миграции повторяемые, не запускают HTTP/publish.
* Для reuse перенести source snapshot с его Bronze, request tasks и control plane из согласованного
  backup. Без этих raw серверный plan покажет missing; само наличие Silver/Gold не заменяет Bronze.
  Далее audit → prepare → coverage → reuse либо разрешённый launch → validate → diagnostics → manual publish.
* Offline regression: `.venv/Scripts/python.exe tests/run_offline_postgres.py` локально,
  `python tests/run_offline_postgres.py` на подготовленном test host. Runner требует локальный
  Docker image postgres:17 и сохранённый pilot fixture; не скачивает image и не делает HTTP.

## Файлы этого изменения

Новые: `dags/taldau_elt/sql/008_multi_year_snapshot.sql`, `dags/taldau_elt/coverage.py`,
`tests/test_multi_year_snapshots.py`, этот README.
Изменены: `dags/taldau_elt/snapshots.py`, `dags/taldau_elt/reports.py`,
`dags/taldau_inv_fixed_assets_2025.py`, `tools/manage_investments_snapshot.py`,
`tests/test_investments_snapshots.py`, `tests/test_snapshot_publish_cli.py`, `tests/run_offline_postgres.py`,
`README_SNAPSHOT_2025.md`. Runner дополнительно принимает имена отдельных unittest tests для адресной проверки.
Миграции 001–007, pilot, legacy, transport loader и docker-compose не переписаны.

## Read-only аудит 18.09.2026

| Год | Месяцев | Numeric cells | x cells | Invalid cells | Raw responses с ключами года |
|---|---:|---:|---:|---:|---:|
|2023|12|461199|38106|0|32926|
|2024|12|515383|29258|0|36199|
|2025|12|651390|28226|0|46436|
|2026|8 (январь–август)|333630|25929|0|38014|

74 518 exact reusable requests/raw responses, 265 territories полностью покрыты сохранённым
деревом, missing=0, expected HTTP=0. Counts по годам не суммируют raw responses:
один ответ может содержать несколько лет. Аудит не публикует и не изменяет source snapshot.
Полные observed period mappings и даты raw сохранены в `data/reports/bronze_2023_2026_audit.json`;
plan — `data/reports/bronze_2023_2026_plan.json`.

## Результат тестов и границы выполненной работы

43 разных offline tests прошли: полный прогон первых 40 (263.454 s), затем 2 новых теста
legacy NULL bounds и возраста reused raw (17.898 s), затем новый тест конфликта Gold period
(10.284 s). После добавления period guard повторно прошли два затронутых positive publish tests.
Все запуски — изолированный PostgreSQL 17, 414 сохранённых pilot raw, без Taldau HTTP;
каждый запуск применял миграции 004–008 дважды. Новые 2023–2026 fixtures не используют production counts.
Пилот, старые snapshot tests и CLI publish остаются совместимыми.

DAG разобран в установленном Airflow 3.3.1 с запретом HTTP: diagnostics — конечная задача,
pool=taldau_api, max_active_tis_per_dag=3, publish в графе отсутствует. Реальные tasks не запускались.

Рабочая БД только читалась: единственный snapshot `kz-investments-2025-v1` остаётся published,
Silver=Gold=651390, 12 periods, 249 numeric territories. Миграция 008 к рабочей БД не применена,
новый snapshot не создан/не опубликован. Состояние подтверждено в
`data/reports/multi_year_working_db_check.json`. На сервер ничего не развёрнуто.
