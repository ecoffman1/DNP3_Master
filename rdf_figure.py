"""
Temporary script — display PV_Power_Plant_1000 Solid data as
one clean (device, field, timestamp, value) row per field for figures.
"""

import json, re, requests, urllib3
from collections import defaultdict
from rdflib import Graph
from solid_server import get_client_credentials, CssAccount
from solid_client_credentials import SolidClientCredentialsAuth, DpopTokenProvider

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with open("component_configs/solid_devices.json") as f:
    SOLID_DEVICES = json.load(f)

DEVICE_KEY   = "PV_Power_Plant_1000"
info         = SOLID_DEVICES[DEVICE_KEY]
DATA_URL     = info["write_dir"].rstrip("/") + "/data.ttl"
SOLID_SERVER = "https://ec2-34-201-119-230.compute-1.amazonaws.com"
NS           = SOLID_SERVER + "/char/dnp3/#"

account = CssAccount(SOLID_SERVER, info["email"], info["password"])
creds   = get_client_credentials(account)
token   = DpopTokenProvider(issuer_url=SOLID_SERVER, client_id=creds.client_id, client_secret=creds.client_secret)
auth    = SolidClientCredentialsAuth(token)

print(f"Fetching {DATA_URL} ...")
resp = requests.get(DATA_URL, auth=auth, verify=False, timeout=15)
if resp.status_code != 200:
    raise SystemExit(f"HTTP {resp.status_code}: {resp.text[:200]}")

g = Graph()
g.parse(data=resp.text, format="turtle")

# Group predicates by subject
by_subject = defaultdict(dict)
for s, p, o in g:
    pred = str(p).replace(NS, "")
    by_subject[str(s)][pred] = str(o)

def last_value(raw: str):
    """Extract the final value from a list literal like '[1.0, 2.0, 3.0]'."""
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        items = [x.strip() for x in raw[1:-1].split(",")]
        return items[-1] if items else raw
    return raw

def last_timestamp(raw: str) -> str:
    """Extract the final timestamp from a list literal."""
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        items = [x.strip().strip("'\"") for x in raw[1:-1].split(",")]
        return items[-1] if items else raw
    return raw.strip("'\"")

# One row per field — keep the subject with the most recent timestamp
latest: dict[str, dict] = {}
for subject, props in by_subject.items():
    if "field" not in props or "value" not in props or "accessed" not in props:
        continue
    field = props["field"]
    ts    = last_timestamp(props["accessed"])
    if field not in latest or ts > latest[field]["ts"]:
        latest[field] = {
            "ts":    ts,
            "value": last_value(props["value"]),
        }

if not latest:
    print("\nNo records found.")
    raise SystemExit()

rows = sorted(latest.items(), key=lambda x: x[0])  # alphabetical by field

fw = max(len(f) for f in latest) + 2
tw = 28
vw = 16

div = "─" * (fw + tw + vw + 8)
print(f"\n  Device:  {DEVICE_KEY}")
print(f"  {div}")
print(f"  {'Field':<{fw}}  {'Last Timestamp':<{tw}}  {'Value'}")
print(f"  {div}")
for field, data in rows:
    print(f"  {field:<{fw}}  {data['ts']:<{tw}}  {data['value']}")
print(f"  {div}")
print(f"  {len(rows)} fields\n")
