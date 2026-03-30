from dnp3_client import DNP3Master, SlaveSession
import threading
import time
from rdf import add_context
import struct

class Translator:
    def __init__(self, ip, port,master_addr, slave_addrs, solid_server):
        self.DNP3_master = DNP3Master(host=ip,port=port,master_addr=master_addr,class0_interval=5.0,integrity_interval=60.0)
        self.solid_server = solid_server

        self.buffer = {}
        self.buffer_lock = threading.Lock()
        
        for addr in slave_addrs:
            slave_session = self.DNP3_master.add_slave(addr)
            slave_session.on_points = self.cbUpdate
    
    def start(self):
        self.DNP3_master.connect()
        self.DNP3_master._startup_sequence()
        threading.Thread(target=self.upload_buffer, daemon=True).start()

        for slave, slave_session in self.DNP3_master.slaves().items():
            self.send_command(slave=slave_session,index=1,turn_on=True)
            time.sleep(2.0)
            self.send_command(slave=slave_session,index=0,turn_on=True)
            
            

    def send_command(self, slave, index, turn_on):
        if turn_on:
            control_code = 0x83  # LATCH_ON + CLOSE bit
        else:
            control_code = 0x84 # LATCH_OFF + CLOSE bit
        crob   = struct.pack('<BBII B', control_code, 1, 100, 100, 0)
        header = bytes([12, 1, 0x28]) + struct.pack("<HH", 1, index)
        
        print(f"Sending SELECT to slave {slave.slave_addr} index {index} turn_on={turn_on}")
        slave._send(0x05, header + crob)

    def cbUpdate(self, points):
        for point in points:
            slave_addr = point[0]
            group = point[1]
            variation = point[2]
            index = point[3]
            value = point[4]
            flags = point[5]
            timestamp = point[6]

            ignore_list = [12]
            if group in ignore_list:
                continue
            threading.Thread(target=self.fill_buffer, args=(slave_addr, group, index, value,timestamp), daemon=True).start()


    def fill_buffer(self, slave_id, group,index, value, timestamp_str):
        if not slave_id in self.buffer:
            self.buffer[slave_id] = {}

        if not group in self.buffer[slave_id]:
            self.buffer[slave_id][group] = {}

        if not index in self.buffer[slave_id][group]:
            self.buffer[slave_id][group][index] = {"values":[],"timestamps":[]}

        with self.buffer_lock:
            self.buffer[slave_id][group][index]["values"].append(value)
            self.buffer[slave_id][group][index]["timestamps"].append(timestamp_str)


    def upload_buffer(self):
        while True:
            time.sleep(60)
            upload_tasks = []
            
            for slave_id, slave_dict in self.buffer.items():
                for group, group_dict in slave_dict.items():
                    for index, index_dict in group_dict.items():
                        with self.buffer_lock:
                            index_copy = index_dict.copy()
                            index_dict["values"] = []
                            index_dict["timestamps"] = []
                        
                        if len(index_copy["values"]) == 0:
                            continue
                        
                        rdf_data = add_context(
                            local_address=slave_id,
                            group=group,
                            index=index,
                            value=index_copy["values"],
                            timestamp=index_copy["timestamps"]
                        )
                        upload_tasks.append(rdf_data)
            
            # Upload sequentially with retry instead of flooding with threads
            for rdf_data in upload_tasks:
                self._upload_with_retry(rdf_data)

    def _upload_with_retry(self, rdf_data, max_retries=3):
        for attempt in range(max_retries):
            try:
                self.solid_server.append(rdf_data)
                return  # success
            except Exception as e:
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"Upload failed (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait)
        print(f"Giving up on upload after {max_retries} attempts")

       