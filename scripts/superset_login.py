"""Save Superset API token from .env credentials to %LOCALAPPDATA%/Temp."""
import json
import os
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env = {}
with open(os.path.join(ROOT, ".env"), encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v

body = json.dumps({
    "username": env.get("SUPERSET_ADMIN_USER", "admin"),
    "password": env["SUPERSET_ADMIN_PASSWORD"],
    "provider": "db",
    "refresh": True,
}).encode()

req = urllib.request.Request(
    "http://localhost:8088/api/v1/security/login",
    data=body, headers={"Content-Type": "application/json"},
)
resp = json.load(urllib.request.urlopen(req))
tmp = os.path.join(os.environ["LOCALAPPDATA"], "Temp")
open(os.path.join(tmp, "tok.txt"), "w").write(resp["access_token"])
print("token saved")
