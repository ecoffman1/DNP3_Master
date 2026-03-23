from rdflib import Graph, Namespace, URIRef, Literal
from config import (
    RESOURCE_URL
)
from load_devices import DEVICE_CONFIGS, ADDR_CONFIG

DNP3 = Namespace(f"{RESOURCE_URL}/#")

def add_context(local_address, group, index, value, data_type, timestamp):
    g = Graph()
    
    device_type = ADDR_CONFIG[local_address]
    device_mapping = DEVICE_CONFIGS[device_type]
    field = device_mapping["groups"][str(group)][str(index)]
    
    if type(timestamp) == list:
        upload_timestamp = timestamp[-1]
    else:
        upload_timestamp = timestamp
        
    reading_uri = URIRef(f"{RESOURCE_URL}/devices/{device_type}/group_{group}/index_{index}/{upload_timestamp}")

    g.add((reading_uri, DNP3.accessed, Literal(timestamp)))
    g.add((reading_uri, DNP3.field, Literal(field)))
    g.add((reading_uri, DNP3.value, Literal(value)))

    return g