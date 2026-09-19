import csv
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


URL = "https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData"

INDICATOR_ID = 701827
PERIOD_ID = 8

KATO_ROOT = "741880"
KRP_ROOT = "741927"
SIF_ROOT = "807855"
GSV_ROOT = "19202525"

DIC_IDS = "68,90,459,4043"

OUTPUT_FILE = Path("data/inv_fixed_assets_2025_taldau.csv")

YEAR = "2025"


session = requests.Session()

retry = Retry(
    total=6,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

session.mount(
    "https://",
    HTTPAdapter(max_retries=retry),
)


def request_tree(terms, term_id, parent_id=""):
    params = {
        "p_measure_id": 1,
        "p_index_id": INDICATOR_ID,
        "p_period_id": PERIOD_ID,
        "p_terms": ",".join(terms),
        "p_term_id": term_id,
        "p_dicIds": DIC_IDS,
        "idx": 3,
        "p_parent_id": parent_id,
    }

    response = session.get(
        URL,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    time.sleep(0.15)

    return response.json()


def walk_dimension(terms, term_id):
    result = []

    roots = request_tree(
        terms=terms,
        term_id=term_id,
        parent_id="",
    )

    def walk(node):
        result.append(node)

        if node.get("leaf") == "true":
            return

        children = request_tree(
            terms=terms,
            term_id=term_id,
            parent_id=node["id"],
        )

        for child in children:
            walk(child)

    for root in roots:
        walk(root)

    return result


def get_2025_periods(node):
    periods = []

    for key, value in node.items():

        if not key.startswith("y"):
            continue

        date_code = key[1:]

        if len(date_code) != 6:
            continue

        if not date_code.endswith(YEAR):
            continue

        reporting_period = node.get(date_code)

        if reporting_period is None:
            continue

        if value in (None, "", "x"):
            continue

        periods.append(
            {
                "period_code": date_code,
                "reporting_period": reporting_period,
                "value": value,
            }
        )

    return periods


def save_rows(rows):
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fieldnames = [
        "kato_id",
        "kato",
        "krp_id",
        "krp",
        "sif_id",
        "sif",
        "gsvziok_id",
        "gsvziok",
        "period_code",
        "reporting_period",
        "value",
    ]

    with OUTPUT_FILE.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)


# =========================================================
# 1. Получаем всё дерево КАТО
# =========================================================

print("Получаем дерево КАТО...")

kato_terms = [
    KATO_ROOT,
    KRP_ROOT,
    SIF_ROOT,
    GSV_ROOT,
]

kato_nodes = walk_dimension(
    terms=kato_terms,
    term_id=KATO_ROOT,
)

print(f"КАТО элементов: {len(kato_nodes)}")


# =========================================================
# 2. Полный обход куба
# =========================================================

rows = []

for kato_number, kato in enumerate(kato_nodes, start=1):

    kato_id = kato["id"]
    kato_name = kato["text"].strip()

    print()
    print(
        f"[{kato_number}/{len(kato_nodes)}] "
        f"КАТО: {kato_name} [{kato_id}]"
    )

    # -----------------------------------------------------
    # КРП
    # -----------------------------------------------------

    krp_terms = [
        kato_id,
        KRP_ROOT,
        SIF_ROOT,
        GSV_ROOT,
    ]

    try:
        krp_nodes = walk_dimension(
            terms=krp_terms,
            term_id=KRP_ROOT,
        )
    except Exception as error:
        print(f"  Ошибка КРП: {error}")
        continue

    print(f"  КРП: {len(krp_nodes)}")

    for krp in krp_nodes:

        krp_id = krp["id"]
        krp_name = krp["text"].strip()

        # -------------------------------------------------
        # СИФ
        # -------------------------------------------------

        sif_terms = [
            kato_id,
            krp_id,
            SIF_ROOT,
            GSV_ROOT,
        ]

        try:
            sif_nodes = walk_dimension(
                terms=sif_terms,
                term_id=SIF_ROOT,
            )
        except Exception as error:
            print(
                f"    Ошибка СИФ "
                f"{krp_name}: {error}"
            )
            continue

        for sif in sif_nodes:

            sif_id = sif["id"]
            sif_name = sif["text"].strip()

            # ---------------------------------------------
            # ГСВЗИОК
            # ---------------------------------------------

            gsv_terms = [
                kato_id,
                krp_id,
                sif_id,
                GSV_ROOT,
            ]

            try:
                gsv_nodes = walk_dimension(
                    terms=gsv_terms,
                    term_id=GSV_ROOT,
                )
            except Exception as error:
                print(
                    f"      Ошибка ГСВЗИОК "
                    f"{krp_name} / "
                    f"{sif_name}: {error}"
                )
                continue

            for gsv in gsv_nodes:

                gsv_id = gsv["id"]
                gsv_name = gsv["text"].strip()

                periods = get_2025_periods(gsv)

                for period in periods:

                    rows.append(
                        {
                            "kato_id": kato_id,
                            "kato": kato_name,
                            "krp_id": krp_id,
                            "krp": krp_name,
                            "sif_id": sif_id,
                            "sif": sif_name,
                            "gsvziok_id": gsv_id,
                            "gsvziok": gsv_name,
                            "period_code": period[
                                "period_code"
                            ],
                            "reporting_period": period[
                                "reporting_period"
                            ],
                            "value": period["value"],
                        }
                    )

    print(
        f"  Накоплено строк: {len(rows)}"
    )

    # сохраняем после каждого КАТО,
    # чтобы при ошибке не потерять прогресс
    save_rows(rows)


# =========================================================
# RESULT
# =========================================================

save_rows(rows)

print()
print("=" * 60)
print("ГОТОВО")
print("=" * 60)

print(f"КАТО обработано: {len(kato_nodes)}")
print(f"Строк за 2025: {len(rows)}")
print(f"Файл: {OUTPUT_FILE}")

print()
print("ETS за 2025: 651365 строк")