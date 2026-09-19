import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


URL = "https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData"

KATO_ID = "268012"          # Г.АСТАНА
KRP_ROOT = "741927"         # Всего
SIF_ROOT = "807855"         # Всего
GSV_ROOT = "19202525"       # Всего

PERIOD_KEY = "122025"       # Декабрь 2025
VALUE_KEY = "y122025"

DIC_IDS = "68,90,459,4043"


# ---------------------------------------------------------
# HTTP session с повторами при 503 / 429 и т.д.
# ---------------------------------------------------------

session = requests.Session()

retry = Retry(
    total=5,
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
        "p_index_id": 701827,
        "p_period_id": 8,
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

    time.sleep(0.2)

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


rows = []


# =========================================================
# 1. КРП
# =========================================================

krp_terms = [
    KATO_ID,
    KRP_ROOT,
    SIF_ROOT,
    GSV_ROOT,
]

krp_nodes = walk_dimension(
    terms=krp_terms,
    term_id=KRP_ROOT,
)

print(f"КРП элементов: {len(krp_nodes)}")


# =========================================================
# 2. Для каждого КРП получаем СИФ
# =========================================================

for krp in krp_nodes:

    krp_id = krp["id"]
    krp_name = krp["text"].strip()

    print()
    print(f"КРП: {krp_name}")

    sif_terms = [
        KATO_ID,
        krp_id,
        SIF_ROOT,
        GSV_ROOT,
    ]

    sif_nodes = walk_dimension(
        terms=sif_terms,
        term_id=SIF_ROOT,
    )

    print(f"  СИФ элементов: {len(sif_nodes)}")


    # =====================================================
    # 3. Для каждого СИФ получаем весь ГСВЗИОК
    # =====================================================

    for sif in sif_nodes:

        sif_id = sif["id"]
        sif_name = sif["text"].strip()

        gsv_terms = [
            KATO_ID,
            krp_id,
            sif_id,
            GSV_ROOT,
        ]

        gsv_nodes = walk_dimension(
            terms=gsv_terms,
            term_id=GSV_ROOT,
        )

        for gsv in gsv_nodes:

            # Если этого периода у комбинации вообще нет,
            # строку не создаём.
            if PERIOD_KEY not in gsv:
                continue

            rows.append({
                "kato": "Г.АСТАНА",
                "krp_id": krp_id,
                "krp": krp_name,
                "sif_id": sif_id,
                "sif": sif_name,
                "gsvziok_id": gsv["id"],
                "gsvziok": gsv["text"].strip(),
                "reporting_period": gsv[PERIOD_KEY],
                "value": gsv.get(VALUE_KEY),
            })


# =========================================================
# RESULT
# =========================================================

numeric_count = sum(
    row["value"] not in (None, "x")
    for row in rows
)

x_count = sum(
    row["value"] == "x"
    for row in rows
)


print()
print("=" * 60)
print("TALDAU RESULT")
print("=" * 60)

print(f"Всего строк: {len(rows)}")
print(f"Числовых значений: {numeric_count}")
print(f"'x' значений: {x_count}")

print()
print("ETS ожидаем: 504 строки")