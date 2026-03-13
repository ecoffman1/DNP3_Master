
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
    # NEW URI STRUCTURE: Includes Group (func_code) to prevent collisions
    # Path: .../slave/1024/timestamp/group_30/reg_0
    reading_id = f"{timestamp}/{field}/"
    reading_uri = URIRef(f"{RESOURCE_URL}/devices/{device_name}/{reading_id}")
    
    g.add((reading_uri, DNP3.device, Literal(device_name)))
    g.add((reading_uri, DNP3.accessed, Literal(timestamp)))
    g.add((reading_uri, DNP3.group, Literal(group)))
    g.add((reading_uri, DNP3.dataindex, Literal(index)))
    g.add((reading_uri, DNP3.field, Literal(field)))
    g.add((reading_uri, DNP3.value, Literal(value)))
    g.add((reading_uri, DNP3.type, Literal(data_type)))

    return g