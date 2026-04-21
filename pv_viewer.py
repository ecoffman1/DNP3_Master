"""
PV Power Plant 1000 — terminal data viewer
Fetches data.ttl from the device's Solid pod and displays readings
for a given time window in a formatted terminal table.

Usage:
    python pv_viewer.py                        # defaults to 09:44–09:45 today
    python pv_viewer.py 09:44 09:45            # explicit start/end (HH:MM)
    python pv_viewer.py 09:44 09:45 2026-04-12 # explicit date too
"""

import json
import os
import re
import sys
from datetime import datetime, date
from collections import defaultdict

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Config — derive DATA_URL from solid_devices.json so it stays in sync
# ---------------------------------------------------------------------------
DEVICE_KEY = "PV_Power_Plant_1000"

_config_path = os.path.join(os.path.dirname(__file__), "component_configs", "solid_devices.json")
with open(_config_path) as _f:
    _solid_devices = json.load(_f)

_write_dir = _solid_devices[DEVICE_KEY]["write_dir"].rstrip("/")
DATA_URL   = f"{_write_dir}/data.ttl"

# Fields to highlight at the top (most useful for a PV plant)
PRIORITY_FIELDS = [
    "Pmeas_kW", "Available_Ppv_kW", "Qmeas_kW", "Smeas_kVA",
    "Vgrid_rms_meas_kV", "Fmeas_Hz", "PFmeas", "State",
]

# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def fetch_turtle(url: str) -> str:
    resp = requests.get(url, verify=False, timeout=30)
    if resp.status_code != 200:
        sys.exit(f"Failed to fetch {url}  ({resp.status_code}): {resp.text[:200]}")
    return resp.text


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------
_TS_PATTERNS = [
    # 2026-04-12T09-44-32-103  (hyphen time + ms)
    re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d+)"),
    # 2026-04-12T09:44:32
    re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"),
]

def parse_timestamp(raw: str) -> datetime | None:
    raw = raw.strip().strip("'\"")
    for pat in _TS_PATTERNS:
        m = pat.search(raw)
        if m:
            g = m.groups()
            ms = int(g[6]) if len(g) > 6 else 0
            ms = min(ms, 999)
            try:
                return datetime(int(g[0]), int(g[1]), int(g[2]),
                                int(g[3]), int(g[4]), int(g[5]), ms * 1000)
            except ValueError:
                pass
    return None


def _extract_quoted(text: str) -> str | None:
    m = re.search(r'"([^"]+)"', text)
    return m.group(1) if m else None


def _parse_value_list(raw: str) -> list[float]:
    """Parse '[1.2, 3.4]' or '5.6' into a list of floats."""
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        tokens = [t.strip().replace("'", "") for t in inner.split(",")]
    else:
        tokens = [raw]
    out = []
    for t in tokens:
        t = t.strip()
        if re.match(r"^-?[\d.eE+]+$", t):
            try:
                out.append(float(t))
            except ValueError:
                pass
        elif t.lower() == "true":
            out.append(1.0)
        elif t.lower() == "false":
            out.append(0.0)
    return out


def _parse_ts_list(raw: str) -> list[str]:
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [t.strip().strip("'\"") for t in inner.split(",")]
    return [raw]


# Each record: {field, group, index, value, timestamp}
Record = dict


def parse_ttl(ttl: str, base_url: str = DATA_URL) -> list[Record]:
    # Strip prefix lines
    clean = re.sub(r"@prefix[^.]*\.\s*", "", ttl)

    # Split on subject IRIs (lines starting with <https://…/dnp3/devices/…>)
    subject_re = re.compile(
        r"(<https?://[^>]+/dnp3/devices/[^>]+>)\s+"
    )
    starts = [(m.start(), m.group(1)) for m in subject_re.finditer(clean)]

    records: list[Record] = []
    for i, (start, subject) in enumerate(starts):
        end = starts[i + 1][0] if i + 1 < len(starts) else len(clean)
        block = clean[start:end]

        # Infer group/index from subject URL
        gm = re.search(r"/group_(\d+)/", subject)
        im = re.search(r"/index_(\d+)/", subject)
        group = int(gm.group(1)) if gm else None
        index = int(im.group(1)) if im else None

        # Extract field, value, accessed
        field_m   = re.search(r"#field>\s*\"([^\"]+)\"", block)
        value_m   = re.search(r"#value>\s*\"([^\"]+)\"", block)
        access_m  = re.search(r"#accessed>\s*\"([^\"]+)\"", block)

        if not field_m or not value_m or not access_m:
            continue

        field      = field_m.group(1)
        values     = _parse_value_list(value_m.group(1))
        timestamps = _parse_ts_list(access_m.group(1))

        if not values:
            continue

        count = max(len(values), len(timestamps))
        for j in range(count):
            v  = values[j]    if j < len(values)     else values[-1]
            ts = timestamps[j] if j < len(timestamps) else timestamps[-1]
            dt = parse_timestamp(ts)
            if dt is None:
                continue
            records.append({
                "field":     field,
                "group":     group,
                "index":     index,
                "value":     v,
                "timestamp": dt,
            })

    return records


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------
def filter_window(records: list[Record], start: datetime, end: datetime) -> list[Record]:
    return [r for r in records if start <= r["timestamp"] < end]


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
DIM    = "\033[2m"


def _colour_value(field: str, value: float) -> str:
    s = f"{value:.4g}"
    if field == "State":
        return (GREEN if value == 1.0 else YELLOW) + s + RESET
    return s


def display(records: list[Record], all_records: list[Record],
            start: datetime, end: datetime) -> None:
    if not records:
        print(f"\n{YELLOW}No data found between "
              f"{start.strftime('%Y-%m-%dT%H:%M:%S')} and "
              f"{end.strftime('%Y-%m-%dT%H:%M:%S')}.{RESET}")
        if all_records:
            ts_all   = [r["timestamp"] for r in all_records]
            ts_min   = min(ts_all)
            ts_max   = max(ts_all)
            print(f"\n{CYAN}  Available data in the file:{RESET}")
            print(f"    Earliest: {ts_min.strftime('%Y-%m-%dT%H:%M:%S')}")
            print(f"    Latest:   {ts_max.strftime('%Y-%m-%dT%H:%M:%S')}")
            print(f"\n  Re-run with:")
            print(f"    python pv_viewer.py {ts_min.strftime('%H:%M')} "
                  f"{ts_max.strftime('%H:%M')} {ts_min.strftime('%Y-%m-%d')}")
        return

    # Aggregate: per field → list of (timestamp, value) sorted by time
    by_field: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for r in records:
        by_field[r["field"]].append((r["timestamp"], r["value"]))
    for lst in by_field.values():
        lst.sort(key=lambda x: x[0])

    # Column widths
    field_w = max(len(f) for f in by_field) + 2
    val_w   = 12
    ts_w    = 21

    sep = "─" * (field_w + val_w + ts_w + 10)

    print()
    print(BOLD + CYAN +
          f"  PV Power Plant 1000 — {start.strftime('%Y-%m-%d')}  "
          f"{start.strftime('%H:%M:%S')} → {end.strftime('%H:%M:%S')}" +
          RESET)
    print(f"  {DIM}Source: {DATA_URL}{RESET}")
    print(f"  {len(records)} data points across {len(by_field)} fields")
    print()

    def print_field_block(field: str, pts: list[tuple[datetime, float]]) -> None:
        values = [v for _, v in pts]
        latest = values[-1]
        mn, mx = min(values), max(values)
        avg    = sum(values) / len(values)

        header = (BOLD + f"  {field:<{field_w}}" + RESET +
                  f"  {len(pts):>3} samples  "
                  f"last={_colour_value(field, latest)}  "
                  f"min={mn:.4g}  max={mx:.4g}  avg={avg:.4g}")
        print(header)

        # Show individual readings (up to 20; summarise if more)
        show = pts if len(pts) <= 20 else pts[:5] + [None] + pts[-5:]  # type: ignore[list-item]
        for item in show:
            if item is None:
                print(f"  {'':>{field_w}}  {DIM}  … {len(pts) - 10} more …{RESET}")
                continue
            ts, v = item
            print(f"  {'':>{field_w}}  {DIM}{ts.strftime('%H:%M:%S.%f')[:-3]}{RESET}"
                  f"  {_colour_value(field, v):<{val_w}}")

    # Priority fields first, then the rest alphabetically
    priority   = [f for f in PRIORITY_FIELDS if f in by_field]
    rest       = sorted(f for f in by_field if f not in PRIORITY_FIELDS)
    all_fields = priority + rest

    if priority:
        print(BOLD + "  ── Key measurements " + "─" * (len(sep) - 22) + RESET)
        for field in priority:
            print_field_block(field, by_field[field])
            print()

    if rest:
        print(BOLD + "  ── All other fields " + "─" * (len(sep) - 22) + RESET)
        for field in rest:
            print_field_block(field, by_field[field])
            print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = sys.argv[1:]
    today = date.today()

    start_str = args[0] if len(args) >= 1 else "09:44"
    end_str   = args[1] if len(args) >= 2 else "09:45"
    date_str  = args[2] if len(args) >= 3 else today.isoformat()

    try:
        start_dt = datetime.fromisoformat(f"{date_str}T{start_str}:00")
        end_dt   = datetime.fromisoformat(f"{date_str}T{end_str}:00")
    except ValueError as e:
        sys.exit(f"Bad date/time argument: {e}")

    print(f"Fetching {DATA_URL} …")
    ttl     = fetch_turtle(DATA_URL)
    records = parse_ttl(ttl)
    print(f"Parsed {len(records)} total records.")

    windowed = filter_window(records, start_dt, end_dt)
    display(windowed, records, start_dt, end_dt)


if __name__ == "__main__":
    main()
