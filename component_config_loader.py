import json

with open("C:\\Users\\ethan\\Documents\\Honors_Thesis\\DNP3_Master\\component_configs\\DER_config.json", 'r') as f:
    component_config = json.load(f)

def getComponentConfig():
    return {
        name: {
            "ip": info["ip_address"], 
            "local_address": info["local_address"],
            "port": info["port"],
        } 
        for name, info in component_config.items()
    }

def getComponentMappings():
    return {
        name: info["groups"] 
        for name, info in component_config.items()
    }