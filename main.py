from master import DNP3_Master
import time
import sys

async def main():
    try:
        client = DNP3_Master()
        client.start()
        print("Master started successfully. Press Ctrl+C to stop.")
        while True:
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



