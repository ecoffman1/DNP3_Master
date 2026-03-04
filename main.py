from master import DNP3_Master
from solid_server import SolidServer
import time
import sys

def main():
    try:
        solid_server = SolidServer()
        solid_server.delete_container("https://ec2-34-201-119-230.compute-1.amazonaws.com/char/dnp3/")
        client = DNP3_Master(solid_server=solid_server)
        client.start()
        print("Master started successfully. Press Ctrl+C to stop.")
        while True:
            client.sendCommand()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping master...")
        DNP3_Master.stopMaster(client)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        sys.exit()


if __name__ == "__main__":
    main()



