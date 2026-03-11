from master import DNP3_Master
from solid_server import SolidServer
import time
import sys
import component_config_loader

device_ids = [1024,1023,1022,1021]


def main():
    component_configs = component_config_loader.getComponentConfig()

    solid_server = SolidServer()
    solid_server.delete_container("https://ec2-34-201-119-230.compute-1.amazonaws.com/char/dnp3/")
    master = DNP3_Master(solid_server=solid_server)
    for device, info in component_configs.items():
        ip_addr = info["ip"]
        slave_address = info["local_address"]
        port_num = info["port"]
        master.create_client(device_name=device, ip_addr=ip_addr, port=port_num, slave_address=slave_address)

    master.start()

    while True:
        time.sleep(100)

if __name__ == "__main__":
    main()



