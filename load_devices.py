import json

with open("component_configs/DER_config.json","r") as f:
    DEVICE_CONFIGS = json.load(f)

with open("component_configs/DER_addr_config.json", "r") as f:
    ADDR_CONFIG = json.load(f)


ADDR_LIST = list(map(int, ADDR_CONFIG))
