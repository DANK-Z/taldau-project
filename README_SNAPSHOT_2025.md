# Snapshot Казахстана за 2025

Поддержка диапазона 2023–2026, offline Bronze reuse, migration 008 и серверные инструкции:
[README_MULTI_YEAR.md](README_MULTI_YEAR.md). Ниже сохранена инструкция исходного snapshot 2025;
его baseline, pilot и имена команд остаются совместимыми.

Snapshot `kz-investments-2025-v1` уже загружен и опубликован пользователем: 265 complete chunks,
651390 numeric rows, 28226 x, 0 invalid/duplicates. Silver и Gold содержат по 651390 строк.
По результатам ручной сверки пользователя delta к ETS составляет +25: август +16, октябрь +9.
Эти counts описывают конкретную версию, а не обязательный результат будущих snapshots.
Изменение публикации не требует повторного extraction или перепубликации этого snapshot.

DAG не имеет расписания, `allow_extraction` по умолчанию false. Новый extraction требует
отдельного подтверждения. Offline-тесты используют только сохранённый Bronze.

Годовой DAG теперь останавливается после **extract → stage → validate → diagnostics**.
После успешной проверки snapshot остаётся `validated`; `ready_to_publish=true` — поле отчёта,
а не автоматическое разрешение на публикацию. `publish` выполняется только отдельной CLI-командой
после просмотра отчёта и отдельного подтверждения пользователя. Это правило действует для всех запусков
данного DAG, включая первый полный 2025 и resume.

История первоначальной подготовки: **22 теста пройдены**, пилот повторно совпал 504/504. Тогда в рабочей базе
остались 414 raw responses, 0 годовых snapshots/chunks; новый DAG paused, 0 запусков.
Зафиксировано в `data/reports/snapshot_2025_readiness.json`.

После изменения на ручную публикацию пройдены **24 offline-теста** и проверен граф DAG
`validate_snapshot → diagnostics → STOP`. Проверка сохранности данных и отсутствия запуска на том этапе:
`data/reports/snapshot_manual_publication_readiness.json`.

## Изменённые и созданные файлы

| Файл | Изменение |
|---|---|
| `dags/taldau_elt/loader.py` | Только вынесен общий конструктор параметров HTTP; поведение пилота сохранено |
| `dags/taldau_elt/snapshots.py` | Новый control plane: snapshot, discovery, chunks, leases, checkpoints, resume |
| `dags/taldau_elt/sql/004_snapshot_model.sql` | Новые таблицы orchestration/staging/diagnostics и SQL-разворот года |
| `dags/taldau_elt/sql/005_snapshot_validation.sql` | Полнота обхода, качество всего snapshot, публикация Silver |
| `dags/taldau_elt/sql/006_ets_reconciliation.sql` | ETS snapshot, проверенный mapping technical IDs, FULL OUTER JOIN |
| `dags/taldau_elt/sql/007_gold_snapshot_publish.sql` | Публикация Gold из Silver, проверка count и двунаправленный EXCEPT |
| `dags/taldau_inv_fixed_assets_2025.py` | Новый manual DAG с Dynamic Task Mapping по территориям и волнами |
| `dags/taldau_elt/reports.py` | Read-only summary, помесячная таблица, quality checks, chunks без фактов |
| `tools/manage_investments_snapshot.py` | CLI migrate/prepare/status/summary/diagnostics/validate/publish/launch |
| `tests/test_investments_snapshots.py` | Offline интеграционные проверки orchestration, SQL и reconciliation |
| `tests/test_snapshot_publish_cli.py` | Совместимость CLI publish и регистрация миграции 007 |
| `tests/run_offline_postgres.py` | Изолированный PostgreSQL для тестов, read-only копирование пилотного Bronze |
| `README_SNAPSHOT_2025.md` | Эта инструкция |

Старый `taldau_pipeline`, его metadata/region_metric таблицы, пилотный DAG, migrations 001–003,
docker-compose, credentials и порты сохранены. В `silver.inv_fixed_assets` добавлен nullable
`source_snapshot_id`; прежние pilot INSERT с явным списком полей совместимы.
Перед миграциями создан backup: `data/backups/20260917_210927_snapshot/before_snapshot_schema.dump`.

## Snapshot и chunks

```text
prepare(snapshot_id, year=2025)                    [только БД, без HTTP]
             |
       подтверждённый launch
             |
discover полного дерева KATO -> raw Bronze -> фиксированный inventory
             |
plan_wave: <=128 незавершённых chunk_id            [маленький XCom]
             |
load_chunk.expand(chunk_id=...)                   [pool taldau_api, <=3 tasks]
             |                                      каждый task: 1 HTTP за раз
             +-- ошибка -> остановка, resume того же snapshot_id
             |
      ещё queued? -- да --> следующий DAG run того же snapshot
             |
            нет
             |
validate всего snapshot -> diagnostics по месяцам
             |
STOP: validated, отчёт готов, новый snapshot ещё не опубликован
             |
отдельное подтверждение пользователя и явная CLI-команда publish
             |
Silver -> Gold за весь 2025 в ОДНОЙ транзакции
             |
отдельная сверка с зафиксированным ETS snapshot
```

`snapshot_id` обозначает общую версию всего года. Конфигурация индикатора фиксируется при `prepare`;
последующее изменение metadata не меняет уже созданную версию. Новый snapshot_id нужен для новой
публикации источника. Текущий код ограничен indicator 701827 / period 8 / year 2025.

`bronze.inv_snapshots` хранит config, discovery_run_id, ожидаемое число chunks, state и timestamps.
`bronze.inv_chunks` хранит territory_id, постоянный run_id, state, attempt, lease/heartbeat,
raw_count, staged_at, completed_at, last_error. Статусы chunk: queued / running / complete / failed.

Пример структуры chunk перед обработкой:

```json
{
  "snapshot_id": "kz-investments-2025-v1",
  "territory_id": 268012,
  "run_id": "kz-investments-2025-v1:kato:268012",
  "state": "queued",
  "attempt": 0
}
```

База назначит числовой chunk_id. Один chunk обходит все КРП → СИФ → ГСВЗИОК для одной территории
и сохраняет все возвращённые периоды. Год фильтрует SQL. KATO inventory включает родителей и детей,
в том числе Казахстан; факты по уровням не суммируются. Уровни администрации по глубине не выдумываются.

## Checkpoint и resume

`bronze.inv_request_tasks` регистрирует точные параметры запроса до HTTP. После транзакционной записи
полного ответа в `bronze.taldau_api_raw` связывает checkpoint с raw_id. Raw text/JSONB остаются
исходными; `x`, parent nodes и данные вне 2025 в них сохраняются.

Стабильный run_id territory chunk использует прежний ключ `UNIQUE(run_id,request_hash)`.
При resume обход начинается с корня, но успешные запросы читаются из PostgreSQL. Сеть используется
только для ещё отсутствующих ответов. Если worker упал между записью raw и обновлением checkpoint,
resume восстановит checkpoint из уже сохранённого ответа.

* Завершённые chunks не включаются в mapping повторно.
* Failed и queued включаются в следующую попытку.
* Running с неистёкшим lease блокирует resume; lease продлевается перед каждым запросом на 15 минут.
* Running с истёкшим lease доступен для восстановления, но PostgreSQL session advisory lock
  дополнительно запрещает перехват ещё живого worker, даже если HTTP retry длился дольше lease.
* Ошибки HTTP сохраняют прогресс. Пустой корректный ответ сохраняется как пустой массив, не как ошибка.
* Один chunk выполняет запросы последовательно с прежними timeout/retry/backoff и паузой 0.2 секунды.
* Предохранители: максимум 20 000 новых запросов на попытку discovery и 5 000 на попытку chunk.
  При достижении лимита попытка падает с сохранённым checkpoint; resume продолжает её.

## Dynamic mapping и XCom

Текущий `max_map_length=1024`, pool `taldau_api` имеет 3 слота. Настройки не изменялись.
Чтобы размер полного KATO не упирался в лимит mapping, каждый DAG run берёт максимум 128 scalar IDs.
Raw JSON, дерево, данные фактов и результаты всех mapped tasks через XCom не передаются.
Mapped task имеет `do_xcom_push=False`, `pool_slots=1`, `max_active_tis_per_dag=3`.

После успешной волны `TriggerDagRunOperator` ставит в очередь следующую волну того же snapshot.
`max_active_runs=1`; ожидания следующего run внутри текущего нет. Идентификатор продолжения
детерминирован по предыдущему run, retry trigger не создаёт дубли.
Если запланированные mapped tasks не завершились, автоматическое продолжение запрещено.
Failed snapshot возобновляется явным повторным launch с **тем же snapshot_id**.

## SQL-преобразование и validation

`staging.stage_inv_chunk(chunk_id)`:

1. Сохраняет observed hierarchy в `bronze.inv_snapshot_members` из raw response + request context.
2. Находит реально присутствующие `yMM2025` и `MM2025` через `jsonb_object_keys`.
3. Сохраняет пары и orphan keys в `staging.inv_year_cells`, включая raw_value=`x`.
4. Преобразует числовые значения в точный PostgreSQL NUMERIC. Python не преобразует факты.

Не создаются декартовы произведения измерений/месяцев. Staging сохраняет дубли для обнаружения,
а не скрывает их через UPSERT. Пара без второй половины — ошибка validation.

```sql
SELECT quality.validate_inv_snapshot('kz-investments-2025-v1');

SELECT check_name, violations
FROM quality.inv_snapshot_checks
WHERE snapshot_id='kz-investments-2025-v1'
ORDER BY check_name;
```

Проверяются:

* Завершённый discovery и совпадение inventory территорий с полным набором chunks.
* Все chunks complete, raw_count соответствует Bronze, staging и completed_at заполнены.
* Каждый raw имеет checkpoint; metadata raw совпадает с замороженным config.
* Для каждого leaf=false есть ответ на точный children request.
* Для каждого наблюдаемого КРП есть корневой запрос СИФ, а для СИФ — ГСВЗИОК.
* NULL keys, orphan pairs, неизвестные нечисловые значения, ошибочные коды месяцев.
* Uniqueness `(indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id)`.
* Конфликтующие parent/name/depth и конфликтующие соответствия period_code/reporting_period.
* Принадлежность фактов территории chunk и наличие справочников для dimension IDs.
* Полностью пустой snapshot блокируется для расследования.

`x` является ожидаемым маркером; он остаётся в Bronze/staging и не попадает в Silver.
Значения NULL/пустая строка/прочие маркеры не считаются автоматически нулём или x.

Полный путь: **extract → stage → validate → diagnostics → manual publish → Silver → Gold**.
Годовой DAG заканчивается на diagnostics; автоматической публикации нет.

`publish_snapshot(conn, snapshot_id) -> int` вызывает `silver.publish_inv_snapshot(text)`, затем
`gold.publish_inv_snapshot(text)` в одной транзакции и проверяет равенство возвращённых counts.
Публикация повторяет validation, берёт общий с пилотом advisory lock
`silver.inv_fixed_assets:pilot`, заменяет только indicator/year snapshot и сверяет ключи/значения
staging ↔ Silver и Silver ↔ Gold в обе стороны через EXCEPT. Gold использует source IDs и surrogate
member_key; повторяющиеся члены snapshot дедуплицируются по `(dimension,member_id)` после проверки
конфликтов иерархии. Отсутствующий member любого из четырёх измерений блокирует Gold, даже если
старый Gold-справочник содержит такой ID.

Gold-функция требует `state='published'`: это подтверждает выполненную публикацию Silver внутри
текущей транзакции либо ранее. Одного `validated` недостаточно. Состояние `published` становится
видно другим транзакциям только после успешного завершения обоих слоёв. Ошибка SQL, потеря строк,
несовпадение ключей/значений или counts откатывают Silver, Gold, справочники и изменение состояния.
Прямой SQL-вызов только Silver не обеспечивает этот контракт; используйте CLI publish.
Повторный publish того же snapshot не создаёт дубликаты; служебные timestamps могут обновляться.

Исторические revisions numeric → x и исчезнувшие
комбинации удаляются только в составе полностью проверенной годовой замены. Более старый snapshot
не может перезаписать более новые опубликованные данные.

## Diagnostics и расхождения с ожиданиями

```sql
SELECT period_code,reporting_period,numeric_rows,x_rows,invalid_rows,expected_rows,delta
FROM quality.inv_month_diagnostics
WHERE snapshot_id='kz-investments-2025-v1'
ORDER BY right(period_code,4),left(period_code,2),reporting_period;
```

Ожидания ETS загружены в `quality.inv_month_expectations`:

| reporting_period | Месяц | rows |
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
|Итого||651365|

Они не входят в blocking validation: корректные современные данные Taldau могут отличаться.
Причины устанавливаются через отчёт нарушений, реальные периоды, x, пустые chunks, затем
построчную ETS-сверку и просмотр source_raw_id. Число строк само по себе не доказывает причину.

```sql
-- Территории без фактов за 2025: повод посмотреть raw, а не создать фиктивные комбинации.
SELECT c.chunk_id,c.territory_id,c.state,count(v.value) AS numeric_rows
FROM bronze.inv_chunks c LEFT JOIN staging.inv_year_cells v USING(chunk_id)
WHERE c.snapshot_id='kz-investments-2025-v1'
GROUP BY 1,2,3 HAVING count(v.value)=0;

-- Дубли с указанием raw-источников.
SELECT indicator_id,reporting_period,kato_id,krp_id,sif_id,gsvziok_id,
       count(*),array_agg(raw_id) AS raw_ids
FROM staging.inv_year_cells WHERE snapshot_id='kz-investments-2025-v1'
GROUP BY 1,2,3,4,5,6 HAVING count(*)>1;
```

## ETS reconciliation

Существующие функции reconciliation сохранены. Они разрешают ссылку на таблицу только при
вызове capture; миграции от наличия ETS не зависят. Для нового ETS baseline:

```sql
SELECT reconciliation.capture_inv_ets(
  'ets-2025-baseline-v1','public.bns_inv_fixed_assets'::regclass,2025
);
```

Capture одной SQL-командой сохраняет неизменённые строки в `reconciliation.ets_inv_raw`, фиксируя
версию сравнения. Исходный space_element_set_id остаётся в этих строках; в Direct Taldau он не придуман.
Для новой версии ETS нужно новое dataset_id.

До сравнения необходимо проверить семантику `kato1`, `krp`, `sif`, `gsvziok` и заполнить
`reconciliation.ets_inv_key_map` подтверждёнными соответствиями **ETS technical key → Taldau source ID**,
с evidence для каждого mapping. Одинаковый числовой вид не является доказательством равенства ID.
Если поля ETS содержат только display names, сначала потребуется исходный справочник ETS с technical IDs
и соответствующая доработка adapter; по названиям ключи не угадываются.

```sql
-- Поля, для которых ещё нет подтверждённого mapping.
SELECT row_id,source_row FROM reconciliation.v_ets_inv_resolved
WHERE dataset_id='ets-2025-baseline-v1'
  AND (kato_id IS NULL OR krp_id IS NULL OR sif_id IS NULL OR gsvziok_id IS NULL)
LIMIT 20;

SELECT reconciliation.compare_inv_ets('kz-investments-2025-v1','ets-2025-baseline-v1');
SELECT * FROM reconciliation.inv_results
WHERE snapshot_id='kz-investments-2025-v1' AND dataset_id='ets-2025-baseline-v1'
ORDER BY reporting_period;
SELECT * FROM reconciliation.inv_differences
WHERE snapshot_id='kz-investments-2025-v1' AND dataset_id='ets-2025-baseline-v1'
ORDER BY reporting_period,kato_id,krp_id,sif_id,gsvziok_id;
```

Функция делает `FULL OUTER JOIN` Direct Silver и ETS по source-ID grain, отдельно считает
missing_in_taldau, extra_in_taldau, value_mismatch, matched, duplicates_taldau, duplicates_ets
и ambiguous_combinations по reporting_period. Дубли сначала считаются, затем сравнение их значений
исключается: они не суммируются и не размножают join. Duplicates — число лишних строк сверх одной на ключ.
Несопоставленные technical keys или неверный numeric ETS блокируют сравнение с понятной ошибкой.
Если Silver уже заменён другой версией, сравнение старого snapshot также блокируется.

## Команды, которые можно использовать после подтверждения

Миграция 007 включена в существующую команду `migrate` после 004–006. На новом сервере сначала
выполните `tools/run_investments_elt.py migrate-bronze` и `migrate-silver` для 001–003, затем
годовой `migrate`. Команды миграций можно повторять; они не запускают extraction или публикацию.
Новая 007 проверяется в изолированной тестовой БД; применение к рабочей БД — отдельная операция.
Действия migrate/prepare/status/diagnostics не делают HTTP-запросов.

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py migrate
.venv/Scripts/python.exe tools/manage_investments_snapshot.py prepare --snapshot-id kz-investments-2025-v1
```

**Ниже команда запуска всей страны — она пока не выполнялась:**

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py launch --snapshot-id kz-investments-2025-v1 --confirm-full-extraction
```

CLI проверяет pool taldau_api (1–3 слота), создаёт snapshot при отсутствии, снимает паузу только с нового
годового DAG и запускает первую волну. Дальше успешные волны продолжаются автоматически до validation
и diagnostics, затем DAG останавливается. Silver автоматически не публикуется.
Gold за весь год этим DAG не перестраивается; пилотный Gold сохранён.

Для resume выполните **эту же launch-команду с тем же snapshot_id**. Другой snapshot_id будет новой
версией с новой загрузкой, а не продолжением. Не запускайте один snapshot одновременно вручную и через DAG.

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py status --snapshot-id kz-investments-2025-v1
.venv/Scripts/python.exe tools/manage_investments_snapshot.py summary --snapshot-id kz-investments-2025-v1 --output data/reports/kz_2025_summary.json
.venv/Scripts/python.exe tools/manage_investments_snapshot.py diagnostics --snapshot-id kz-investments-2025-v1 --output data/reports/kz_2025_diagnostics.json
```

`summary` печатает читаемые итоги и таблицу; `status`/`diagnostics` возвращают JSON того же отчёта.
`--output` всегда сохраняет полный JSON. Все три команды выполняют только чтение, не запускают
validation и не меняют состояние. Их можно использовать и после неуспешной проверки.

Отчёт содержит snapshot_id, snapshot_state, ready_to_publish, territories_total, chunks_total,
chunks_complete/failed/running/queued, raw_responses, numeric_rows, x_rows, invalid_rows,
duplicate_keys, duplicate_rows, expected_ets_rows, delta и число проваленных blocking checks.
`months` содержит period_code, reporting_period, numeric_rows, x_rows, invalid_rows, expected_ets_rows, delta.
`checks` перечисляет все blocking checks с violations; `chunks_without_facts` показывает территории
без числовых фактов, включая состояние chunk. Незавершённый chunk пока не доказывает отсутствие данных.

Итоги и месяцы рассчитываются из staging одним SQL statement, поэтому могут использоваться до
публикации Silver и представляют согласованное состояние на момент чтения. numeric_rows — строки
с числовым value; invalid_rows — строки с неизвестным значением, неполной парой period/value,
NULL ключами, неверным периодом или scope. Строка может одновременно иметь число и структурную ошибку;
numeric_rows и invalid_rows в таком случае не являются непересекающимися категориями. Дубли считаются отдельно.

После успешной validation задача `diagnostics` автоматически сохраняет JSON в существующий volume:
`data/reports/inv_snapshot_<первые 16 символов SHA256 snapshot_id>_summary.json`.
Полный путь записывается в лог задачи. Через XCom большой отчёт не передаётся.
Если validation завершилась ошибкой, blocking checks уже сохранены, а отчёт доступен через CLI summary.

После прерванного SQL-этапа validation можно повторить без HTTP:

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py validate --snapshot-id kz-investments-2025-v1
```

**Только после просмотра итогов и отдельного подтверждения публикации:**

```powershell
.venv/Scripts/python.exe tools/manage_investments_snapshot.py publish --snapshot-id kz-investments-2025-v1
```

Существующая команда publish повторяет blocking validation и публикует **Silver + Gold** транзакционно.
Порядок аргументов `--snapshot-id ... publish` также поддерживается; JSON-ответ остаётся
`{"published_rows": N}`. Функции миграции не запускают публикацию самостоятельно.
Диагностические ETS counts по-прежнему не блокируют публикацию.

Не удалять volumes, не выполнять `docker compose down -v`.

## Offline-проверки

```powershell
.venv/Scripts/python.exe tests/run_offline_postgres.py
```

Runner читает fixture `astana-2025-12-pilot-v1` из источника PG* (по умолчанию localhost:5434)
в read-only транзакции. Создаёт отдельный PostgreSQL 17 из уже локального Docker image (`--pull never`),
использует tmpfs без постоянного volume и случайный свободный localhost-порт. Применяет существующие
команды миграций, повторяет годовую миграцию для проверки идемпотентности, восстанавливает пилот
только внутри тестовой БД. После тестов останавливает только собственный временный контейнер.
Рабочая БД не получает INSERT/UPDATE/DELETE или миграций. Docker и локальный image postgres:17 обязательны.

Тесты запрещают HTTP и работают на синтетическом двухтерриториальном inventory плюс
копии сохранённого Bronze Астаны. Все записи и публикации тестов откатываются. Проверяются 12 месяцев,
x, grain, hierarchy, checkpoint, lease/advisory locks, ограничение mapping, отказ от частичной публикации,
diagnostic-only counts и FULL OUTER JOIN на синтетическом ETS. Добавлены равенство Silver/Gold
по counts и natural keys + values, повторный publish, сохранение другого года, missing member,
отказ Gold до публикации Silver, повторная validation, SQL row loss и mismatch с откатом обоих слоёв,
Python count mismatch с откатом, обратная совместимость CLI. Pilot SQL проверяется существующим
тестом точного NUMERIC и `gold.refresh_inv_pilot`; оба WHERE в EXCEPT корректны.

Проверка изменения Gold-публикации 18.09.2026: **32 теста прошли**, без пропусков,
на отдельном PostgreSQL 17; годовые миграции успешно применены дважды. Рабочий snapshot
не перепубликовывался, миграция 007 к рабочей БД в рамках этой проверки не применялась.

Это проверка механизма на fixtures; результаты реального snapshot описаны в начале документа.
Большой snapshot — последовательность наблюдений API во времени, не атомарный снимок удалённого сервиса.

Годовой и пилотный DAG при добавлении публикации Gold не изменяются; HTTP extraction не запускается.

Документация: [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html),
[TriggerDagRunOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/trigger_dagrun/index.html).
