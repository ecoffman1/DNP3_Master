
import uuid
from rdflib import Graph, Namespace, URIRef, Literal
from config import (
    RESOURCE_URL
)


DNP3 = Namespace(f"{RESOURCE_URL}/#")

def add_context(slave_id, register, function, func_code, value, data_type, notes, timestamp):
    g = Graph()
    g.bind("ns1", DNP3) 
    
    # NEW URI STRUCTURE: Includes Group (func_code) to prevent collisions
    # Path: .../slave/1024/timestamp/group_30/reg_0
    reading_id = f"{timestamp}/group_{func_code}/reg_{register}"
    reading_uri = URIRef(f"{RESOURCE_URL.rstrip('/')}/slave/{slave_id}/{reading_id}")
    
    g.add((reading_uri, DNP3.accessed, Literal(timestamp)))
    g.add((reading_uri, DNP3.register, Literal(register)))
    g.add((reading_uri, DNP3.function, Literal(function)))
    g.add((reading_uri, DNP3.func_code, Literal(func_code)))
    g.add((reading_uri, DNP3.value, Literal(value)))
    g.add((reading_uri, DNP3.type, Literal(data_type)))
    g.add((reading_uri, DNP3.notes, Literal(notes)))
    
    return g