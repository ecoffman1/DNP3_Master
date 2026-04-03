from dnp3_client import DNP3Master, SlaveSession
import threading
import time
import queue
from rdf import add_context
from load_devices import ADDR_CONFIG
import struct

UPLOAD_WORKERS = 4        # concurrent upload threads
UPLOAD_BATCH_INTERVAL = 60  # seconds between buffer flushes

class Translator:
    def __init__(self, ip, port, master_addr, slave_addrs, solid_server):
        self.DNP3_master = DNP3Master(host=ip, port=port, master_addr=master_addr, class0_interval=5.0, integrity_interval=60.0)
        self.solid_server = solid_server

        self.buffer = {}
        self.buffer_lock = threading.Lock()
        self._upload_queue = queue.Queue()

        for addr in slave_addrs:
            slave_session = self.DNP3_master.add_slave(addr)
            slave_session.on_points = self.cbUpdate

    def start(self):
        self.DNP3_master.connect()
        self.DNP3_master._startup_sequence()

        threading.Thread(target=self.upload_buffer, daemon=True).start()
        for _ in range(UPLOAD_WORKERS):
            threading.Thread(target=self._upload_worker, daemon=True).start()

        for _, slave_session in self.DNP3_master.slaves().items():
            self.send_command(slave=slave_session, index=1, turn_on=True)
            time.sleep(2.0)
            self.send_command(slave=slave_session, index=0, turn_on=True)
            self.send_command(slave=slave_session, index=1, turn_on=False)
            

    def send_command(self, slave, index, turn_on):
        control_code = 0x83 if turn_on else 0x84  # LATCH_ON / LATCH_OFF + CLOSE bit
        crob   = struct.pack('<BBII B', control_code, 1, 100, 100, 0)
        header = bytes([12, 1, 0x28]) + struct.pack("<HH", 1, index)
        print(f"Sending SELECT to slave {slave.slave_addr} index {index} turn_on={turn_on}")
        slave._send(0x05, header + crob)

    def cbUpdate(self, points):
        for point in points:
            slave_addr = point[0]
            group      = point[1]
            index      = point[3]
            value      = point[4]
            timestamp  = point[6]

            if group == 12:
                continue
            self.fill_buffer(slave_addr, group, index, value, timestamp)

    def fill_buffer(self, slave_id, group, index, value, timestamp_str):
        with self.buffer_lock:
            if slave_id not in self.buffer:
                self.buffer[slave_id] = {}
            if group not in self.buffer[slave_id]:
                self.buffer[slave_id][group] = {}
            if index not in self.buffer[slave_id][group]:
                self.buffer[slave_id][group][index] = {"values": [], "timestamps": []}
            self.buffer[slave_id][group][index]["values"].append(value)
            self.buffer[slave_id][group][index]["timestamps"].append(timestamp_str)

    def upload_buffer(self):
        while True:
            time.sleep(UPLOAD_BATCH_INTERVAL)

            with self.buffer_lock:
                snapshot = {}
                for slave_id, slave_dict in self.buffer.items():
                    for group, group_dict in slave_dict.items():
                        for index, index_dict in group_dict.items():
                            if not index_dict["values"]:
                                continue
                            snapshot[(slave_id, group, index)] = {
                                "values":     index_dict["values"][:],
                                "timestamps": index_dict["timestamps"][:],
                            }
                            index_dict["values"] = []
                            index_dict["timestamps"] = []

            for (slave_id, group, index), data in snapshot.items():
                device_type = ADDR_CONFIG[str(slave_id)]["device_type"]
                device_key = f"{device_type}_{slave_id}"
                rdf_data = add_context(
                    local_address=slave_id,
                    group=group,
                    index=index,
                    value=data["values"],
                    timestamp=data["timestamps"],
                )
                self._upload_queue.put((rdf_data, device_key))

    def _upload_worker(self):
        while True:
            rdf_data, device_key = self._upload_queue.get()
            try:
                self._upload_with_retry(rdf_data, device_key)
            finally:
                self._upload_queue.task_done()

    def _upload_with_retry(self, rdf_data, device_key, max_retries=3):
        for attempt in range(max_retries):
            try:
                self.solid_server.append(rdf_data, device_key)
                return
            except Exception as e:
                print(f"Upload failed (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        print(f"Giving up on upload after {max_retries} attempts")
