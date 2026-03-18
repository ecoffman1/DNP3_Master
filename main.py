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
    devices = []
    for device in component_configs:
        local_address = device
        port = component_configs[device]["port"]
        devices.append(DNP3_Master(solid_server=solid_server,ip_addr="10.1.114.34", port=port, slave_addr=local_address))


    print(devices)
    while True:
        for master in devices:
            master.start()
            time.sleep(1.5)
            master.stopMaster()

if __name__ == "__main__":
    main()



