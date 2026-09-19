import json
import sys

import requests


BASE_URL = "https://taldau.stat.gov.kz/ru/Api"


if len(sys.argv) < 2:
    print(
        "Использование:"
        "\npython inspect_indicator.py 703831"
    )
    raise SystemExit(1)


INDEX_ID = int(sys.argv[1])


# =========================================================
# 1. Получаем доступные периоды
# =========================================================

response = requests.get(
    f"{BASE_URL}/GetPeriodList",
    params={
        "indexId": INDEX_ID
    },
    timeout=30,
)

response.raise_for_status()

periods = response.json()


print()
print("=== PERIODS ===")

print(
    json.dumps(
        periods,
        ensure_ascii=False,
        indent=2
    )
)


# =========================================================
# 2. Для каждого периода получаем разрезности
# =========================================================

for period in periods:

    period_id = period["id"]
    period_name = period["name"]

    print()
    print("=" * 70)

    print(
        f"{period_name} "
        f"(period_id={period_id})"
    )

    response = requests.get(
        f"{BASE_URL}/GetSegmentList",
        params={
            "indexId": INDEX_ID,
            "periodId": period_id,
        },
        timeout=30,
    )

    response.raise_for_status()

    segments = response.json()

    print(
        json.dumps(
            segments,
            ensure_ascii=False,
            indent=2
        )
    )