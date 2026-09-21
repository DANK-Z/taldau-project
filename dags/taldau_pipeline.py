"""Legacy/deprecated region-metric DAG retained for compatibility."""
from datetime import timedelta
from pathlib import Path
import sys

_DAG_DIR = str(Path(__file__).resolve().parent)
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="taldau_pipeline",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="Asia/Almaty"
    ),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["taldau", "data-engineering", "legacy", "deprecated"],
)
def taldau_pipeline():

    @task
    def get_indicators():

        import psycopg2
        from airflow.hooks.base import BaseHook

        dwh_conn = BaseHook.get_connection(
            "digest_target_db"
        )

        conn = psycopg2.connect(
            host=dwh_conn.host,
            port=dwh_conn.port,
            dbname=dwh_conn.schema,
            user=dwh_conn.login,
            password=dwh_conn.password,
        )

        try:

            with conn.cursor() as cursor:

                cursor.execute("""
                    SELECT
                        indicator_id,
                        indicator_name,
                        target_name,
                        period_id,
                        terms,
                        term_id,
                        dic_ids,
                        idx,
                        parent_id
                    FROM taldau.metadata_taldau_indicators
                    WHERE is_active = TRUE
                    ORDER BY indicator_id;
                """)

                rows = cursor.fetchall()

                indicators = []

                for row in rows:

                    indicators.append({
                        "indicator_id": row[0],
                        "indicator_name": row[1],
                        "target_name": row[2],
                        "period_id": row[3],
                        "terms": row[4],
                        "term_id": row[5],
                        "dic_ids": row[6],
                        "idx": row[7],
                        "parent_id": row[8],
                    })

                if not indicators:
                    raise ValueError(
                        "В taldau.metadata_taldau_indicators "
                        "нет активных показателей."
                    )

                print(
                    f"Найдено активных показателей: "
                    f"{len(indicators)}"
                )

                for config in indicators:
                    print(
                        config["indicator_id"],
                        config["indicator_name"]
                    )

                return indicators

        finally:

            conn.close()

    # =========================================================
    # 1. EXTRACT
    # =========================================================

    @task(
        pool="taldau_api"
    )
    def extract(config):

        import json
        from pathlib import Path

        import requests

        BASE_URL = "https://taldau.stat.gov.kz/ru/Api"

        INDEX_ID = config["indicator_id"]
        PERIOD_ID = config["period_id"]

        TERMS = config["terms"]
        TERM_ID = config["term_id"]

        DIC_IDS = config["dic_ids"]

        IDX = config["idx"]
        PARENT_ID = config["parent_id"]

        raw_dir = Path("/opt/airflow/data/raw")

        raw_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------
        # Получаем периоды
        # -------------------------

        periods_params = {
            "p_measure_id": 1,
            "p_index_id": INDEX_ID,
            "p_period_id": PERIOD_ID,
            "p_terms": TERMS,
            "p_term_id": TERM_ID,
            "p_dicIds": DIC_IDS,
        }

        periods_response = requests.get(
            f"{BASE_URL}/GetIndexPeriods",
            params=periods_params,
            timeout=30,
        )

        periods_response.raise_for_status()

        periods = periods_response.json()

        periods_file = (
            raw_dir
            / f"{INDEX_ID}_periods.json"
        )

        with open(
            periods_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                periods,
                file,
                ensure_ascii=False,
                indent=2,
            )

        print(
            f"Периоды сохранены: {periods_file}"
        )

        # -------------------------
        # Получаем регионы
        # -------------------------

        regions_params = {
            "p_measure_id": 1,
            "p_index_id": INDEX_ID,
            "p_period_id": PERIOD_ID,
            "p_terms": TERMS,
            "p_term_id": TERM_ID,
            "p_dicIds": DIC_IDS,
            "idx": IDX,
            "p_parent_id": PARENT_ID,
        }

        regions_response = requests.get(
            f"{BASE_URL}/GetIndexTreeData",
            params=regions_params,
            timeout=30,
        )

        regions_response.raise_for_status()

        regions = regions_response.json()

        regions_file = (
            raw_dir
            / f"{INDEX_ID}_regions.json"
        )

        with open(
            regions_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                regions,
                file,
                ensure_ascii=False,
                indent=2,
            )

        print(
            f"Indicator: {INDEX_ID}"
        )

        print(
            f"Регионов: {len(regions)}"
        )

        return {
            "config": config,
            "periods_file": str(periods_file),
            "regions_file": str(regions_file),
        }

    # =========================================================
    # 2. TRANSFORM
    # =========================================================

    @task
    def transform(payload):

        import json
        from pathlib import Path
        from datetime import datetime

        config = payload["config"]

        INDEX_ID = config[
            "indicator_id"
        ]

        silver_dir = Path(
            "/opt/airflow/data/silver"
        )

        silver_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        periods_file = Path(
            payload["periods_file"]
        )

        regions_file = Path(
            payload["regions_file"]
        )

        # -------------------------
        # Читаем периоды
        # -------------------------

        with open(
            periods_file,
            "r",
            encoding="utf-8"
        ) as file:
            periods = json.load(file)

        # -------------------------
        # Читаем регионы
        # -------------------------

        with open(
            regions_file,
            "r",
            encoding="utf-8"
        ) as file:
            regions = json.load(file)

        # -------------------------
        # Создаём mapping:
        #
        # 122025 -> 31.12.2025
        # -------------------------

        date_mapping = dict(
            zip(
                periods["dateList"],
                periods["datesToDraw"]
            )
        )

        # -------------------------
        # Wide -> Long
        # -------------------------

        records = []

        for region in regions:

            region_id = int(
                region["id"]
            )

            region_name = (
                region["text"]
            )

            for key, value in region.items():

                if not key.startswith("y"):
                    continue

                date_code = key[1:]

                date_string = (
                    date_mapping.get(
                        date_code
                    )
                )

                if date_string is None:
                    continue

                if value in (
                    None,
                    ""
                ):
                    continue

                period_date = (
                    datetime.strptime(
                        date_string,
                        "%d.%m.%Y"
                    ).date()
                )

                records.append({
                    "indicator_id": INDEX_ID,
                    "region_id": region_id,
                    "region_name": region_name,
                    "period_date": (
                        period_date.isoformat()
                    ),
                    "value": value,
                })

        # -------------------------
        # Сортируем
        # -------------------------

        records.sort(
            key=lambda row: (
                row["region_id"],
                row["period_date"]
            )
        )

        # -------------------------
        # Сохраняем Silver JSON
        # -------------------------

        silver_file = (
            silver_dir
            / f"{INDEX_ID}_region.json"
        )

        with open(
            silver_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                records,
                file,
                ensure_ascii=False,
                indent=2
            )

        print(
            f"Indicator {INDEX_ID}: "
            f"{len(records)} строк"
        )

        return {
            "config": config,
            "silver_file": str(
                silver_file
            ),
            "row_count": len(records),
        }

    # =========================================================
    # 3. LOAD SILVER
    # =========================================================

    @task
    def load_silver(payload):

        import json

        from pathlib import Path
        from decimal import Decimal
        from datetime import date

        import psycopg2
        from airflow.hooks.base import BaseHook
        from psycopg2.extras import execute_values

        # -------------------------
        # Читаем Silver JSON
        # -------------------------

        silver_file = Path(
            payload["silver_file"]
        )

        config = payload["config"]

        with open(
            silver_file,
            "r",
            encoding="utf-8"
        ) as file:
            data = json.load(file)

        print(
            f"Строк прочитано из Silver JSON: "
            f"{len(data)}"
        )

        # -------------------------
        # Подготавливаем записи
        # -------------------------

        records = []

        for row in data:

            records.append(
                (
                    int(row["indicator_id"]),
                    int(row["region_id"]),
                    row["region_name"],
                    date.fromisoformat(
                        row["period_date"]
                    ),
                    Decimal(
                        row["value"]
                    ),
                )
            )

        print(
            f"Подготовлено к загрузке: "
            f"{len(records)}"
        )

        # -------------------------
        # Подключаемся к DWH
        # -------------------------

        dwh_conn = BaseHook.get_connection(
            "digest_target_db"
        )

        conn = psycopg2.connect(
            host=dwh_conn.host,
            port=dwh_conn.port,
            dbname=dwh_conn.schema,
            user=dwh_conn.login,
            password=dwh_conn.password,
        )

        # -------------------------
        # UPSERT
        # -------------------------

        insert_sql = """
            INSERT INTO taldau.silver_statistics_region (
                indicator_id,
                region_id,
                region_name,
                period_date,
                value
            )
            VALUES %s

            ON CONFLICT (
                indicator_id,
                region_id,
                period_date
            )
            DO UPDATE SET
                region_name = EXCLUDED.region_name,
                value = EXCLUDED.value,
                loaded_at = NOW();
        """

        # -------------------------
        # Transaction
        # -------------------------

        try:

            with conn.cursor() as cursor:

                execute_values(
                    cursor,
                    insert_sql,
                    records
                )

            conn.commit()

            print(
                f"Silver успешно загружен. "
                f"Строк обработано: {len(records)}"
            )

            return config

        except Exception:

            conn.rollback()

            print(
                "Ошибка загрузки Silver. "
                "Выполнен ROLLBACK."
            )

            raise

        finally:

            conn.close()

            print(
                "Соединение с PostgreSQL закрыто."
            )

    # =========================================================
    # 4. VALIDATE SILVER
    # =========================================================

    @task
    def validate_silver(indicators):

        import psycopg2
        from airflow.hooks.base import BaseHook

        dwh_conn = BaseHook.get_connection(
            "digest_target_db"
        )

        conn = psycopg2.connect(
            host=dwh_conn.host,
            port=dwh_conn.port,
            dbname=dwh_conn.schema,
            user=dwh_conn.login,
            password=dwh_conn.password,
        )

        try:

            with conn.cursor() as cursor:

                for config in indicators:

                    indicator_id = config[
                        "indicator_id"
                    ]

                    indicator_name = config[
                        "indicator_name"
                    ]

                    print(
                        f"Проверяем: "
                        f"{indicator_name} "
                        f"({indicator_id})"
                    )

                    # Проверяем количество строк
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM taldau.silver_statistics_region
                        WHERE indicator_id = %s;
                        """,
                        (indicator_id,)
                    )

                    row_count = cursor.fetchone()[0]

                    print(
                        f"Строк: {row_count}"
                    )

                    if row_count == 0:

                        raise ValueError(
                            f"Нет данных для "
                            f"indicator_id="
                            f"{indicator_id}"
                        )

                    # Проверяем NULL
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM taldau.silver_statistics_region
                        WHERE indicator_id = %s
                          AND (
                              region_id IS NULL
                              OR period_date IS NULL
                              OR value IS NULL
                          );
                        """,
                        (indicator_id,)
                    )

                    null_count = cursor.fetchone()[0]

                    print(
                        f"NULL: {null_count}"
                    )

                    if null_count > 0:

                        raise ValueError(
                            f"Обнаружено NULL: "
                            f"{null_count}"
                        )

                    # Проверяем дубликаты
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM (
                            SELECT
                                indicator_id,
                                region_id,
                                period_date
                            FROM taldau.silver_statistics_region

                            WHERE indicator_id = %s

                            GROUP BY
                                indicator_id,
                                region_id,
                                period_date

                            HAVING COUNT(*) > 1
                        ) duplicates;
                        """,
                        (indicator_id,)
                    )

                    duplicate_count = (
                        cursor.fetchone()[0]
                    )

                    print(
                        f"Дубликаты: "
                        f"{duplicate_count}"
                    )

                    if duplicate_count > 0:

                        raise ValueError(
                            f"Обнаружены дубликаты "
                            f"для indicator_id="
                            f"{indicator_id}"
                        )

                print(
                    "Все Data Quality "
                    "проверки пройдены ✅"
                )

        finally:
            conn.close()

    # =========================================================
    # 5. BUILD GOLD
    # =========================================================

    @task
    def build_gold(indicators):

        import psycopg2

        from airflow.hooks.base import BaseHook

        # =========================================================
        # Подключение
        # =========================================================

        dwh_conn = BaseHook.get_connection(
            "digest_target_db"
        )

        conn = psycopg2.connect(
            host=dwh_conn.host,
            port=dwh_conn.port,
            dbname=dwh_conn.schema,
            user=dwh_conn.login,
            password=dwh_conn.password,
        )

        try:

            with conn.cursor() as cursor:

                # =================================================
                # 1. GOLD SCHEMA
                # =================================================

                cursor.execute("""
                    CREATE SCHEMA IF NOT EXISTS taldau;
                """)

                # =================================================
                # 2. DIM REGION
                # =================================================

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS taldau.gold_dim_region (
                        region_key BIGINT
                            GENERATED ALWAYS AS IDENTITY
                            PRIMARY KEY,

                        source_region_id BIGINT
                            NOT NULL
                            UNIQUE,

                        region_name TEXT
                            NOT NULL
                    );
                """)

                cursor.execute("""
                    INSERT INTO taldau.gold_dim_region (
                        source_region_id,
                        region_name
                    )

                    SELECT DISTINCT
                        region_id,
                        region_name

                    FROM taldau.silver_statistics_region

                    ON CONFLICT (source_region_id)

                    DO UPDATE SET
                        region_name =
                            EXCLUDED.region_name;
                """)

                # =================================================
                # 3. DIM INDICATOR
                # =================================================

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS taldau.gold_dim_indicator (
                        indicator_key BIGINT
                            GENERATED ALWAYS AS IDENTITY
                            PRIMARY KEY,

                        source_indicator_id BIGINT
                            NOT NULL
                            UNIQUE,

                        indicator_name TEXT
                            NOT NULL
                    );
                """)

                indicator_rows = [
                    (
                        config["indicator_id"],
                        config["indicator_name"]
                    )
                    for config in indicators
                ]

                cursor.executemany(
                    """
                    INSERT INTO taldau.gold_dim_indicator (
                        source_indicator_id,
                        indicator_name
                    )

                    VALUES (%s, %s)

                    ON CONFLICT (
                        source_indicator_id
                    )

                    DO UPDATE SET
                        indicator_name =
                            EXCLUDED.indicator_name;
                    """,
                    indicator_rows
                )

                # =================================================
                # 4. DIM DATE
                # =================================================

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS taldau.gold_dim_date (
                        date_key INTEGER PRIMARY KEY,
                        full_date DATE NOT NULL UNIQUE,
                        year INTEGER NOT NULL
                    );
                """)

                cursor.execute("""
                    INSERT INTO taldau.gold_dim_date (
                        date_key,
                        full_date,
                        year
                    )

                    SELECT DISTINCT
                        TO_CHAR(
                            period_date,
                            'YYYYMMDD'
                        )::INTEGER,

                        period_date,

                        EXTRACT(
                            YEAR FROM period_date
                        )::INTEGER

                    FROM taldau.silver_statistics_region

                    ON CONFLICT (date_key)
                    DO NOTHING;
                """)

                # =================================================
                # 5. FACT TABLE
                # =================================================

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS
                        taldau.gold_fact_statistics (

                        indicator_key BIGINT NOT NULL,
                        region_key BIGINT NOT NULL,
                        date_key INTEGER NOT NULL,

                        value NUMERIC(38, 6),

                        loaded_at TIMESTAMPTZ
                            NOT NULL
                            DEFAULT NOW(),

                        PRIMARY KEY (
                            indicator_key,
                            region_key,
                            date_key
                        ),

                        FOREIGN KEY (indicator_key)
                            REFERENCES
                                taldau.gold_dim_indicator(
                                    indicator_key
                                ),

                        FOREIGN KEY (region_key)
                            REFERENCES
                                taldau.gold_dim_region(
                                    region_key
                                ),

                        FOREIGN KEY (date_key)
                            REFERENCES
                                taldau.gold_dim_date(
                                    date_key
                                )
                    );
                """)

                # =================================================
                # 6. SILVER -> GOLD FACT
                # =================================================

                cursor.execute("""
                    INSERT INTO taldau.gold_fact_statistics (
                        indicator_key,
                        region_key,
                        date_key,
                        value
                    )

                    SELECT
                        i.indicator_key,
                        r.region_key,
                        d.date_key,
                        s.value

                    FROM taldau.silver_statistics_region s

                    JOIN taldau.gold_dim_region r
                        ON s.region_id =
                           r.source_region_id

                    JOIN taldau.gold_dim_indicator i
                        ON s.indicator_id =
                           i.source_indicator_id

                    JOIN taldau.gold_dim_date d
                        ON s.period_date =
                           d.full_date

                    ON CONFLICT (
                        indicator_key,
                        region_key,
                        date_key
                    )

                    DO UPDATE SET
                        value =
                            EXCLUDED.value,

                        loaded_at =
                            NOW();
                """)

                # =================================================
                # 7. Небольшая проверка
                # =================================================

                cursor.execute("""
                    SELECT
                        i.source_indicator_id,
                        COUNT(*)

                    FROM taldau.gold_fact_statistics f

                    JOIN taldau.gold_dim_indicator i
                        ON f.indicator_key =
                           i.indicator_key

                    GROUP BY
                        i.source_indicator_id

                    ORDER BY
                        i.source_indicator_id;
                """)

                result = cursor.fetchall()

                print(
                    "Gold rows по показателям:"
                )

                for row in result:

                    print(
                        f"indicator_id={row[0]}, "
                        f"rows={row[1]}"
                    )

            conn.commit()

            print(
                "Gold Star Schema успешно обновлена."
            )

        except Exception:

            conn.rollback()

            print(
                "Ошибка построения Gold. "
                "Выполнен ROLLBACK."
            )

            raise

        finally:

            conn.close()

            print(
                "Соединение с PostgreSQL закрыто."
            )

    @task
    def validate_gold():

        import psycopg2
        from airflow.hooks.base import BaseHook

        dwh_conn = BaseHook.get_connection(
            "digest_target_db"
        )

        conn = psycopg2.connect(
            host=dwh_conn.host,
            port=dwh_conn.port,
            dbname=dwh_conn.schema,
            user=dwh_conn.login,
            password=dwh_conn.password,
        )

        try:

            with conn.cursor() as cursor:

                # =================================================
                # 1. Количество строк в Silver по показателям
                # =================================================

                cursor.execute("""
                    SELECT
                        indicator_id,
                        COUNT(*)
                    FROM taldau.silver_statistics_region
                    GROUP BY indicator_id
                    ORDER BY indicator_id;
                """)

                silver_rows = dict(
                    cursor.fetchall()
                )

                print("Silver:")

                for indicator_id, count in silver_rows.items():

                    print(
                        f"indicator_id={indicator_id}, "
                        f"rows={count}"
                    )

                # =================================================
                # 2. Количество строк в Gold по показателям
                # =================================================

                cursor.execute("""
                    SELECT
                        i.source_indicator_id,
                        COUNT(*)

                    FROM taldau.gold_fact_statistics f

                    JOIN taldau.gold_dim_indicator i
                        ON f.indicator_key =
                           i.indicator_key

                    GROUP BY
                        i.source_indicator_id

                    ORDER BY
                        i.source_indicator_id;
                """)

                gold_rows = dict(
                    cursor.fetchall()
                )

                print("Gold:")

                for indicator_id, count in gold_rows.items():

                    print(
                        f"indicator_id={indicator_id}, "
                        f"rows={count}"
                    )

                # =================================================
                # 3. Сравниваем Silver и Gold
                # =================================================

                errors = []

                for indicator_id, silver_count in silver_rows.items():

                    gold_count = gold_rows.get(
                        indicator_id,
                        0
                    )

                    if silver_count != gold_count:

                        errors.append(
                            (
                                indicator_id,
                                silver_count,
                                gold_count
                            )
                        )

                # =================================================
                # 4. Если есть расхождения — валим task
                # =================================================

                if errors:

                    print(
                        "Обнаружены расхождения "
                        "между Silver и Gold:"
                    )

                    for (
                        indicator_id,
                        silver_count,
                        gold_count
                    ) in errors:

                        print(
                            f"indicator_id={indicator_id}: "
                            f"Silver={silver_count}, "
                            f"Gold={gold_count}"
                        )

                    raise ValueError(
                        "Gold содержит не все данные "
                        "из Silver."
                    )

                print(
                    "Gold Data Quality проверки "
                    "пройдены ✅"
                )

        finally:

            conn.close()

    # =========================================================
    # TASK DEPENDENCIES
    # =========================================================

    indicators = get_indicators()

    extract_tasks = extract.expand(
        config=indicators
    )

    transform_tasks = transform.expand(
        payload=extract_tasks
    )

    silver_tasks = load_silver.expand(
        payload=transform_tasks
    )

    validate_task = validate_silver(
        indicators
    )

    gold_task = build_gold(
        indicators
    )

    validate_gold_task = validate_gold()

    (
        silver_tasks
        >> validate_task
        >> gold_task
        >> validate_gold_task
    )


taldau_pipeline()
