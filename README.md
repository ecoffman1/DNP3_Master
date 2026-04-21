# DNP3 Master — Setup & Running Guide

A DNP3 master station that polls DER (Distributed Energy Resource) devices over TCP, stores readings to per-device Solid Pods, and listens for commands written back by a dashboard.

---

## Prerequisites

- Python 3.10+
- A running [Community Solid Server (CSS)](https://github.com/CommunitySolidServer/CommunitySolidServer) instance (local or remote)
- A Typhoon HIL simulation with DNP3 outstations configured and running
- Network access from this machine to both the HIL TCP endpoint and the Solid server

---

## 1. Create a Virtual Environment

```bash
python -m venv venv
```

Activate it:

- **Windows:** `venv\Scripts\activate`
- **Mac/Linux:** `source venv/bin/activate`

---

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 3. Configure Environment Variables

Create a `.env` file in the project root:

```env
# DNP3 TCP connection to the Typhoon HIL outstation router
REMOTE_IP=192.168.x.x
REMOTE_PORT=20000
MASTER_ADDR=1

# Solid Pod server base URL
SOLID_SERVER=https://<your-css-instance>

# Base URL for device data resources
RESOURCE_URL=https://<your-css-instance>

# Commands resource URL (legacy — per-device URLs are in solid_devices.json)
COMMANDS_URL=https://<your-css-instance>/commands

# OIDC issuer (usually the same as SOLID_SERVER for CSS)
OIDC_ISSUER=https://<your-css-instance>

# Utility/portal account credentials (used for ACL provisioning)
CSS_EMAIL=admin@example.com
CSS_PASSWORD=your-password

# WebID of the portal/dashboard user that writes commands to device pods
PORTAL_WEB_ID=https://<your-css-instance>/portal/profile/card#me
```

---

## 4. Configure Devices

### `component_configs/DER_addr_config.json`

Maps each DNP3 outstation address to a device type:

```json
{
  "1000": {"device_type": "PV_Power_Plant"},
  "1004": {"device_type": "Battery"},
  "1008": {"device_type": "Wind_Power_Plant"}
}
```

Supported device types: `PV_Power_Plant`, `Battery`, `Wind_Power_Plant`, `Diesel_Generator`

### `component_configs/solid_devices.json`

Defines the Solid Pod account and URLs for each device. Each key must match the pattern `{device_type}_{address}`:

```json
{
  "PV_Power_Plant_1000": {
    "email": "pvpp_1000@example.com",
    "password": "password123",
    "webId": "https://<css>/PV_Power_Plant_1000/profile/card#me",
    "write_dir": "https://<css>/PV_Power_Plant_1000/dnp3/",
    "commands_url": "https://<css>/PV_Power_Plant_1000/dnp3_commands/PV_Power_Plant_1000"
  }
}
```

Add one entry per device, matching every address in `DER_addr_config.json`.

---

## 5. Run the Program

```bash
python main.py
```

On first run (or when adding new devices), `provision_devices()` will:
1. Register a Solid Pod account for each device (skips if already exists)
2. Authenticate and cache a DPoP token per device
3. Set ACLs so the portal account can write commands to each device pod

Subsequent runs re-authenticate and re-apply ACLs (both operations are idempotent).

---

## Architecture Overview

| Component | Role |
|-----------|------|
| `dnp3_client.py` | Raw DNP3 TCP master — polls outstations, decodes frames |
| `translator.py` | Bridges DNP3 points to Solid — buffers readings, uploads every 60s, listens for commands |
| `solid_server.py` | Solid Pod client — authentication, SPARQL PATCH uploads, WebSocket command subscriptions |
| `rdf.py` | Converts DNP3 point data to RDF/Turtle graphs |
| `component_configs/DER_config.json` | Maps DNP3 group/index numbers to human-readable signal names per device type |
| `component_configs/DER_addr_config.json` | Maps outstation addresses to device types |
| `component_configs/solid_devices.json` | Per-device Solid Pod credentials and URLs |

---

## Polling Behaviour

- **Class 0 poll:** every 5 seconds (all static data)
- **Integrity poll:** every 60 seconds (Class 0+1+2+3)
- **Upload to Solid:** accumulated readings flushed every 60 seconds, uploaded concurrently by a worker pool

---

## Troubleshooting

| Symptom | Likely Cause |
|---------|--------------|
| `401 UnauthorizedHttpError` on WebSocket or command fetch | `provision_devices()` commented out in `main.py` — uncomment it |
| `auth=None` in WebSocket subscription log | Same as above — auth tokens never built |
| Device cycling between startup/running | DNP3 Enable command not completing — check SELECT vs DIRECT_OPERATE in `send_command` |
| Solid upload timeouts | Too many devices for sequential uploads — increase `UPLOAD_WORKERS` in `translator.py` |
