
import component_config_loader
from rdflib import Graph, Namespace, URIRef, Literal
from config import (
    RESOURCE_URL
)

device_mappings = component_config_loader.getComponentMappings()
DNP3 = Namespace(f"{RESOURCE_URL}/#")

def add_context(local_address, group, index, value, data_type, timestamp):
    g = Graph()
    
    device_mapping = device_mappings[local_address]
    device_name = device_mapping["device_name"]
    field = device_mapping["groups"][str(group)][str(index)]
    if type(timestamp) == list:
        upload_timestamp = timestamp[-1]
    else:
        upload_timestamp = timestamp
        
    reading_uri = URIRef(f"{RESOURCE_URL}/devices/{device_name}/group_{group}/index_{index}/{upload_timestamp}")

    g.add((reading_uri, DNP3.accessed, Literal(timestamp)))
    g.add((reading_uri, DNP3.field, Literal(field)))
    g.add((reading_uri, DNP3.value, Literal(value)))
    g.add((reading_uri, DNP3.type, Literal(data_type)))

    return g