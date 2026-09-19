import json
import sys

import requests


BASE_URL = "https://taldau.stat.gov.kz/ru/Api"

if len(sys.argv) < 2:
    print(
        "Использование:"
        '\npython .\\tools\\discover_indicator.py "Инвестиции в основной капитал"'
    )
    raise SystemExit(1)


keyword = " ".join(sys.argv[1:])


print(f"Поиск: {keyword}")

response = requests.get(
    f"{BASE_URL}/Search",
    params={
        "keyword": keyword
    },
    timeout=30,
)

response.raise_for_status()

data = response.json()


print()
print("=== SEARCH RESULTS ===")
print()

print(
    json.dumps(
        data,
        ensure_ascii=False,
        indent=2
    )
)
