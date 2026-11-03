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


d = get("dashboard/")
print("dashboards:", d["count"])
for r in d["result"]:
    print("-", r["dashboard_title"])

d = get("dataset/")
print("datasets:")
for r in d["result"]:
    print("-", r["table_name"], "columns:", len(r.get("columns") or []))
