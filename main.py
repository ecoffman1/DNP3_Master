from translator import Translator
from solid_server import SolidServer
import time
import sys
from load_devices import ADDR_LIST
from config import REMOTE_IP, REMOTE_PORT, MASTER_ADDR



def main():
    solid_server = SolidServer()
    solid_server.delete_container("https://ec2-34-201-119-230.compute-1.amazonaws.com/char/dnp3/")
    translator = Translator(ip=REMOTE_IP, port=REMOTE_PORT, master_addr=MASTER_ADDR, slave_addrs=ADDR_LIST, solid_server=solid_server)
    translator.start()

if __name__ == "__main__":
    main()



