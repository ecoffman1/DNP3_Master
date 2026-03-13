from master import DNP3_Master, DNP3_Device
from solid_server import SolidServer
import time
import sys
import component_config_loader

device_ids = [1024,1023,1022,1021]


def main():
    component_configs = component_config_loader.getComponentConfig()

    devices = []
    for device in component_configs:
        local_address = device
        port = component_configs[device]["port"]
        devices.append(DNP3_Device(local_address=local_address, port=port))

    solid_server = SolidServer()
    solid_server.delete_container("https://ec2-34-201-119-230.compute-1.amazonaws.com/char/dnp3/")
    
    while True:
        for device in devices:
            list = [device]
            master = DNP3_Master(solid_server=solid_server,ip_addr="10.1.114.34",port=20000, devices=list)
            master.start()
            time.sleep(1.5)
            master.stopMaster()

if __name__ == "__main__":
    main()



