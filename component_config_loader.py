import json

with open("C:\\Users\\ethan\\Documents\\Honors_Thesis\\DNP3_Master\\component_configs\\DER_config.json", 'r') as f:
    component_config = json.load(f)

def getComponentConfig():
    return {
        int(address): {
            "ip": info["ip_address"], 
            "port": info["port"],
        } 
        for address, info in component_config.items()
    }

def getComponentMappings():
    return {
        int(address): info
        for address, info in component_config.items()
    }