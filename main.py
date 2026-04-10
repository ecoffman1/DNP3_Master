from translator import Translator
from solid_server import SolidServer
import time
import sys
from load_devices import ADDR_LIST
from config import REMOTE_IP, REMOTE_PORT, MASTER_ADDR

def main():
    solid_server = SolidServer()
    # solid_server.provision_devices()
    translator = Translator(ip=REMOTE_IP, port=REMOTE_PORT, master_addr=MASTER_ADDR, slave_addrs=ADDR_LIST, solid_server=solid_server)
    translator.start()
    translator.start_command_listeners()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()



