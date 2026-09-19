import time
import requests


BASE_URL = "https://taldau.stat.gov.kz/ru/Api/GetIndexTreeData"

COMMON_PARAMS = {
    "p_measure_id": 1,
    "p_index_id": 701827,
    "p_period_id": 8,
    "p_terms": "249809,19123,451902,19202525",
    "p_term_id": 19202525,
    "p_dicIds": "68,90,459,4043",
    "idx": 3,
}


session = requests.Session()


def get_children(parent_id):
    params = COMMON_PARAMS.copy()
    params["p_parent_id"] = parent_id

    response = session.get(
        BASE_URL,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


def walk(parent_id, depth=0):
    children = get_children(parent_id)

    for child in children:
        child_id = child["id"]
        name = child["text"].strip()
        is_leaf = child.get("leaf") == "true"

        print(
            "    " * depth
            + f"├── {name} "
            + f"[id={child_id}, leaf={is_leaf}]"
        )

        if not is_leaf:
            time.sleep(0.2)
            walk(child_id, depth + 1)


print("ГСВЗИОК")
walk(19202525)