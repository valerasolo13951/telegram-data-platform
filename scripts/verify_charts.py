"""Сквозная проверка: запрос витрины через chart-data API Superset."""
import json
import os
import urllib.request

TMP = os.path.join(os.environ["LOCALAPPDATA"], "Temp")
tok = open(os.path.join(TMP, "tok.txt")).read().strip()


def get(path):
    req = urllib.request.Request(
        f"http://localhost:8088/api/v1/{path}",
        headers={"Authorization": f"Bearer {tok}"},
    )
    return json.load(urllib.request.urlopen(req))


def post(path, payload):
    req = urllib.request.Request(
        f"http://localhost:8088/api/v1/{path}",
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
    )
    return json.load(urllib.request.urlopen(req))


datasets = get("dataset/")
first = datasets["result"][0]
ds_id = first["id"]
print("dataset:", first["table_name"], "columns in detail:",
      len(get(f"dataset/{ds_id}")["result"]["columns"]))

payload = {
    "datasource": {"id": ds_id, "type": "table"},
    "queries": [{
        "columns": ["activity_date", "channel_id", "messages_count"],
        "row_limit": 5,
        "orderby": [{"activity_date": False}],
    }],
}
resp = post("chart/data", payload)
rows = resp["result"][0]["data"]
print("chart-data rows:", len(rows))
for r in rows[:3]:
    print(" ", r)
