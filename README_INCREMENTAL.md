# Taldau: исторический и инкрементальный запуск

Изменения предназначены для Apache Airflow 2.9.2 и PostgreSQL. Установка кода, миграция,
создание индекса, публикация и снятие DAG с паузы — отдельные операционные действия.

## Два DAG

| Свойство | `taldau_statistics_2023_2026` | `taldau_statistics_incremental` |
|---|---|---|
| Диапазон | Всегда 2023–2026 | Год logical date в Asia/Almaty либо явный диапазон |
| Расписание | Ручной запуск | 20-го числа, 07:00 Asia/Almaty (`0 7 20 * *`) |
| Первый импорт | На паузе | На паузе |
| Публикация | Только отдельным действием | Выключена, включается `auto_publish=true` |
| Извлечение по умолчанию | Требует `allow_extraction=true` | Разрешено после ручного включения DAG |

Оба DAG используют общую оркестрацию: discovery → dynamic mapping волн до 128 chunk ID →
validation → diagnostics. Pool `taldau_api` должен иметь ровно 3 слота; задачи discovery/load_chunk
занимают один слот и ограничены `max_active_tis_per_dag=3`. `max_active_runs=1`, `catchup=False`.
Ни один код в этом изменении не снимает новый DAG с паузы.

Расписание использует `CronTriggerTimetable`, поэтому logical date соответствует моменту запуска,
а не началу предыдущего месячного интервала. Январский запуск обрабатывает январский год.
См. [документацию Airflow о timetable](https://airflow.apache.org/docs/apache-airflow/2.10.4/authoring-and-scheduling/timetable.html#timetables-comparisons);
поведение отдельно проверяется тестом в поддерживаемом runtime 2.9.2.

## Параметры нового DAG

| Параметр | По умолчанию | Назначение |
|---|---|---|
| `year_start`, `year_end` | `null`, `null` | Оба пропущены: только логический год. Для backfill передавать оба |
| `batch_id` | `""` | Автоматически: локальная logical date + SHA-256 от logical date/run ID |
| `resume_failed` | `false` | Повторно поставить failed chunks в очередь в исходном batch |
| `allow_extraction` | `true` | При `false` authorization завершается до HTTP |
| `auto_publish` | `false` | Разрешает публикацию только после успешных проверок |

Поддерживаемый домен — целые годы 2023–2100, в соответствии с существующими generic SQL CHECK.
`year_start > year_end`, один заданный конец диапазона и годы вне домена отклоняются.
Это техническая граница представления, а не ежегодно обновляемый горизонт registry.

Идентификатор batch стабилен для retry одного run, различается для разных run ID и содержит только
`A-Za-z0-9_-`. Continuation передаёт уже полученный ID через XCom, оба года и `auto_publish`.
Она не создаёт новый snapshot и не сбрасывает существующий DAG run. Не используйте ID исторического
batch в incremental DAG: такая попытка отклоняется.

## Что означает bounded refresh

Каждый **новый batch** создаёт новые snapshots с замороженными source_config, диапазоном и
`incremental_as_of` (локальная дата первоначального запуска). Discovery читает реальные деревья API.
`bronze_snapshot_available_periods` показывает обнаруженные пары period/value в этом диапазоне;
инвентарь дополняется по мере обхода более глубоких измерений.

Taldau возвращает периоды вместе в JSON tree response: существующий endpoint не имеет параметров
начала/конца года. Поэтому lossless Bronze сохраняет полный ответ API, в том числе его исторические
ключи. SQL staging извлекает только заданные годы. При обычном запуске не создаются snapshots,
staging или публикация 2023–2026; обещания уменьшить размер самого HTTP-ответа нет.

SQL использует только ключи, фактически присутствующие в ответе. Будущие месяцы и кварталы после
cutoff не попадают в staging; на 20 сентября допустимы завершённые месяцы до августа и кварталы
до II квартала. Население с `point_in_time_start_period` использует начало года: его реальный
annual-код `12YYYY` допустим с января. Отсутствующие периоды не генерируются. Неполные пары и
неизвестные значения в допустимом периоде остаются нарушениями качества.

Внутри одного snapshot retry/resume использует завершённые raw/request checkpoints, не меняя
их содержимое. Завершённые chunks не загружаются повторно. Следующий новый scheduled batch
делает свежие запросы: revised значения могут заменить предыдущий snapshot выбранного года.
Raw из старого batch автоматически не переиспользуется. Для получения исправления некорректного
ответа источника нужен новый batch, а не retry старого неизменяемого raw.

В январе новый batch имеет scope только нового года. Поздние исправления предыдущего года
обрабатываются отдельным явным backfill. Если для года ещё нет числовых данных по показателю,
проверка empty_snapshot блокирует публикацию; пустой snapshot не очищает Silver/Gold.

## Первый запуск и backfill

До первого запуска: применить согласованную миграцию 012, проверить DagBag и pool `taldau_api`,
оставить новый DAG на паузе до согласованного окна. `is_paused_upon_creation` действует лишь при
первом создании записи DAG, а не при каждом обновлении кода. Старый historical DAG не переименовывать.

В согласованное окно снять incremental DAG с паузы вручную. Обычный запуск можно отправить через
Airflow UI с `{}`: он выполнит extraction, validation, diagnostics и остановится без публикации.
Ручные запуски paused DAG не исполняют задачи до снятия с паузы. Не используйте старую CLI-команду
`manage_statistics_batch.py launch` для incremental: она намеренно осталась исторической, 2023–2026.

Пример ручного backfill через Trigger DAG (JSON conf):

```json
{"year_start": 2026, "year_end": 2027, "auto_publish": false}
```

Чтобы протестировать scope 2027 в staging, задайте logical date 2027 и помните, что cutoff отражает
эту дату. Для operational backfill используйте фактическую дату запуска и явные годы.

## Возобновление и владение batch

Уникальная запись `taldau.metadata_batch_owners` закрепляет incremental DAG за одним batch на все
волны. Advisory transaction lock сериализует получение этой записи. Новый scheduled/manual batch
отклоняется, пока предыдущий не завершён. Это предотвращает чередование разных batch между волнами,
которое само по себе `max_active_runs=1` не исключает.

После успешных diagnostics и необязательной публикации final task освобождает запись. Ошибка
validation, diagnostics, trigger или publication сохраняет владение, чтобы следующий месяц
не маскировал незавершённую обработку. DAG run с неуспешной валидацией завершается ошибкой.

Для возобновления укажите исходный ID из `authorize_batch` XCom или `bronze_batches`:

```json
{"batch_id": "taldau-inc-ORIGINAL-ID", "resume_failed": true, "auto_publish": false}
```

Без ID `resume_failed` отклоняется. Если годы не указаны, для существующего batch берутся сохранённые
годы, даже после смены календарного года. Сохранённый cutoff остаётся прежним. Изменить scope нельзя.
Повторный запуск уже опубликованного batch сохраняет published-состояние.

Если immutable raw повреждён или первый год ещё не опубликован источником, resume не сможет
исправить его. Для отказа от такого batch: поставить DAG на паузу, остановить/завершить все его
running и queued runs (включая continuation), проверить отсутствие активных workers/leases,
зафиксировать причину отказа. Только после этого оператор может адресно освободить владельца:

```sql
-- :batch_id — точный ID отказавшего batch, не шаблон.
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('taldau_statistics_incremental',0));
DELETE FROM taldau.metadata_batch_owners o
WHERE o.owner_key='taldau_statistics_incremental' AND o.batch_id=:batch_id
  AND NOT EXISTS (
    SELECT 1 FROM taldau.bronze_chunks c JOIN taldau.bronze_snapshots s USING(snapshot_id)
    WHERE s.batch_id=o.batch_id AND c.state='running' AND c.lease_until>clock_timestamp());
COMMIT;
```

SQL-проверка leases не заменяет остановку discovery и очередей Airflow. Сами batches, snapshots,
raw, staging и опубликованные данные при отказе не удаляются. После проверки создайте новый batch.

## Публикация и проверки

Чтобы опубликовать уже проверенный batch, повторно запустите его с явным ID:

```json
{"batch_id": "taldau-inc-ORIGINAL-ID", "auto_publish": true}
```

Перед публикацией нужны batch `validated`/`published`, все snapshots `validated`/`published`
с `validated_at`, существующие blocking checks с нулём нарушений и ноль invalid/missing rows.
Функции `taldau.publish_snapshot()` вызываются **последовательно в одной транзакции batch**.
Каждая повторно валидирует snapshot, заменяет только его indicator/year scope и проверяет число
строк и двусторонний Silver/Gold content EXCEPT. Эти проверки выполняются до COMMIT; для нового
snapshot они не могут выполняться до INSERT, когда данных Silver/Gold ещё нет.
Ошибка любого показателя откатывает весь batch и его published-флаг. Защита от публикации более
старой revision поверх новой остаётся действующей.

Diagnostics сохраняются в `TALDAU_REPORT_DIR` (по умолчанию `/opt/airflow/data/reports`). Для SQL ниже
`:batch_id` — параметр клиента; используйте свой ID, а не исторический production batch.

```sql
SELECT b.batch_id,b.state,s.indicator_key,s.snapshot_id,s.year_start,s.year_end,s.state
FROM taldau.bronze_batches b JOIN taldau.bronze_snapshots s USING(batch_id)
WHERE b.batch_id=:batch_id ORDER BY s.indicator_key;

SELECT s.indicator_key,q.check_name,q.violations
FROM taldau.quality_snapshot_checks q JOIN taldau.bronze_snapshots s USING(snapshot_id)
WHERE s.batch_id=:batch_id AND q.severity='blocking' AND q.violations<>0;

SELECT s.indicator_key,d.*
FROM taldau.quality_period_diagnostics d JOIN taldau.bronze_snapshots s USING(snapshot_id)
WHERE s.batch_id=:batch_id ORDER BY s.indicator_key,d.period_code;

SELECT s.indicator_key,
 (SELECT count(*) FROM taldau.staging_observation_cells v
  WHERE v.snapshot_id=s.snapshot_id AND v.value_status='numeric') AS staging_numeric,
 (SELECT count(*) FROM taldau.silver_observations v
  WHERE v.source_snapshot_id=s.snapshot_id) AS silver_rows,
 (SELECT count(*) FROM taldau.gold_fact_observations v
  WHERE v.source_snapshot_id=s.snapshot_id) AS gold_rows
FROM taldau.bronze_snapshots s WHERE s.batch_id=:batch_id;

WITH silver AS (
 SELECT indicator_key,reporting_period,coordinates,value FROM taldau.silver_observations
 WHERE source_snapshot_id IN (SELECT snapshot_id FROM taldau.bronze_snapshots WHERE batch_id=:batch_id)
), gold AS (
 SELECT indicator_key,reporting_period,coordinates,value FROM taldau.gold_fact_observations
 WHERE source_snapshot_id IN (SELECT snapshot_id FROM taldau.bronze_snapshots WHERE batch_id=:batch_id)
)
SELECT count(*) AS content_mismatches FROM (
 (SELECT * FROM silver EXCEPT ALL SELECT * FROM gold)
 UNION ALL
 (SELECT * FROM gold EXCEPT ALL SELECT * FROM silver)
) differences;
```

После публикации ожидаются одинаковые staging_numeric/Silver/Gold counts и `content_mismatches=0`.
До публикации Silver/Gold нового snapshot равны нулю. После замены более новой revision старый
snapshot перестаёт владеть текущими фактами; сравнивайте текущую опубликованную версию.

## Deploy и rollback

1. Отдельно согласовать резервирование и окно миграции. На существующей схеме 010/011 достаточно
   применить `012_incremental_refresh.sql`. Bootstrap/CLI миграции включают её после 011.
2. Индекс `gold_fact_observations_lookup_idx` создаётся идемпотентно на
   `(indicator_key, coordinates, reporting_period) INCLUDE (value)`. На большой production-таблице
   согласовать блокировки и длительность `CREATE INDEX`; создание индекса — отдельное разрешение.
3. Доставить новые файлы и удалить именно три retired DAG-файла из deployment-каталога, иначе
   старые копии продолжат обнаруживаться. Legacy SQL/таблицы остаются. Убедиться, что старые DAG
   не имеют running/queued задач; исторические Airflow записи не обязательно исчезнут из UI сразу.
4. Проверить DagBag: ровно historical и incremental; проверить connection `digest_target_db`
   без вывода credentials, pool 3, timezone, paused-состояние и params.
5. Согласовать первый extraction-only запуск и проверить diagnostics. Затем отдельно решить
   вопрос `auto_publish` и включения регулярного расписания.

Rollback кода: поставить incremental на паузу, остановить/завершить его задачи, вернуть прежние
версии DAG/shared Python через Git из известной стабильной revision. Аддитивную миграцию,
таблицы и индекс можно оставить. Historical DAG и batch `taldau-statistics-2023-2026-prod-v2`
сохраняют исходные ID, scope и frozen config; investment-only SQL 008, snapshots.py, coverage.py
не переписаны.

Rollback данных — отдельная проверенная операция из резервной копии либо восстановительного
snapshot с новой revision на нужный indicator/year. Старый snapshot нельзя просто publish поверх
новой revision: существующая защита правильно отклоняет такой rollback. До включения auto_publish
текущие Silver/Gold не меняются.

## Тесты без production

Windows unit checks требуют `psycopg2`, `requests`, `tzdata` в `.venv`; Linux Airflow содержит
системную базу часовых поясов. Полный suite без DB:

```powershell
$env:TALDAU_TEST_DB='0'
$env:TALDAU_LEGACY_TEST_DATABASE=''
.venv/Scripts/python.exe -m unittest discover -s tests -v
```

Настоящие DagBag checks следует запускать в Airflow 2.9.2 (Linux), например в официальном образе
`apache/airflow:2.9.2-python3.11`, с `--network none`, отключёнными DB-тестами и read-only mount
только каталогов `dags`, `tests`, `tools`. Без Airflow runtime эти checks явно skipped.

```powershell
.venv/Scripts/python.exe tests/run_synthetic_postgres.py
```

Этот runner создаёт одноразовый PostgreSQL 17 с tmpfs и изолированной `TALDAU_TEST_DB`, применяет
SQL только туда, создаёт синтетический raw fixture и удаляет контейнер после тестов. Не подключается
к существующим БД и не читает `.env`. Он включает generic regression, incremental SQL и fresh-schema
checks; в том числе race-condition `test_chunk_failure_waits_for_wave_before_failing_snapshot`.
Для старых investment integration tests нужен отдельный утверждённый cached pilot fixture; новый
runner его не копирует из локальной или production БД. `run_offline_postgres.py` автоматически
не запускать: прежний runner читает источник через PG*.
