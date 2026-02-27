from rdflib import Graph, Namespace, URIRef, Literal
from config import (
    SOLID_SERVER
)
MODBUS = Namespace(f"{SOLID_SERVER}/pnataraj/modbus/#")

def add_context(slave_id, register, function, func_code, value, data_type, notes, timestamp):
    g = Graph()
    modbus_device = URIRef(f"{SOLID_SERVER}/pnataraj/modbus/slave/{slave_id}")
    g.add((modbus_device, MODBUS.register, Literal(register)))
    g.add((modbus_device, MODBUS.function, Literal(function)))
    g.add((modbus_device, MODBUS.func_code, Literal(func_code)))
    g.add((modbus_device, MODBUS.value, Literal(value)))
    g.add((modbus_device, MODBUS.type, Literal(data_type)))
    g.add((modbus_device, MODBUS.notes, Literal(notes)))
    g.add((modbus_device, MODBUS.accessed, Literal(timestamp)))

    return g.serialize(format="turtle")