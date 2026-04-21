"""Delete data.ttl from every device's dnp3/ container."""
import json, requests, urllib3
from solid_server import get_client_credentials, CssAccount
from solid_client_credentials import SolidClientCredentialsAuth, DpopTokenProvider

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOLID_SERVER = "https://ec2-34-201-119-230.compute-1.amazonaws.com"

with open("component_configs/solid_devices.json") as f:
    SOLID_DEVICES = json.load(f)

EMPTY_TTL = "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"

for device_key, info in SOLID_DEVICES.items():
    write_dir = info["write_dir"].rstrip("/")
    data_url = f"{write_dir}/data.ttl"

    try:
        account = CssAccount(SOLID_SERVER, info["email"], info["password"])
        creds = get_client_credentials(account)
        token = DpopTokenProvider(
            issuer_url=SOLID_SERVER,
            client_id=creds.client_id,
            client_secret=creds.client_secret,
        )
        auth = SolidClientCredentialsAuth(token)
    except Exception as e:
        print(f"[{device_key}] Auth failed: {e}")
        continue

    resp = requests.delete(data_url, auth=auth, verify=False, timeout=15)
    if resp.status_code in (200, 204, 205, 404):
        print(f"[{device_key}] Cleared ({resp.status_code})")
    else:
        print(f"[{device_key}] Delete failed ({resp.status_code}): {resp.text[:100]}")
