from dnp3_client import DNP3Master, SlaveSession
import threading
import time
from rdf import add_context

class Translator:
    def __init__(self, ip, port,master_addr, slave_addrs, solid_server):
        self.DNP3_master = DNP3Master(host=ip,port=port,master_addr=master_addr)
        self.solid_server = solid_server

        self.buffer = {}
        self.buffer_lock = threading.Lock()
        threading.Thread(target=self.upload_buffer, daemon=True).start()

        for addr in slave_addrs:
            slave_session = self.DNP3_master.add_slave(addr)
            slave_session.on_points = self.cbUpdate
    
    def start(self):
        self.DNP3_master.connect()
        self.DNP3_master._startup_sequence()

    def cbUpdate(self, points):
        for point in points:
            slave_addr = point[0]
            group = point[1]
            variation = point[2]
            index = point[3]
            value = point[4]
            flags = point[5]
            timestamp = point[6]
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
        time.sleep(60)
        for slave_id, slave_dict in self.buffer.items():
            for group, group_dict in slave_dict.items():
                for index, index_dict in group_dict.items():
                    with self.buffer_lock:
                        index_copy = index_dict.copy()
                        index_dict["values"] = []
                        index_dict["timestamps"] = []
                    if len(index_copy["values"]) == 0:
                        print("NOT ENOUGH VALUES \n\n")
                        continue

                    rdf_data = add_context(
                        local_address=slave_id,
                        group=group,
                        index=index,
                        value=index_copy["values"],
                        data_type=index_copy["data_type"],
                        timestamp=index_copy["timestamps"]
                    )

                    threading.Thread(target=self.solid_server.append, args=(rdf_data,), daemon=True).start()

        self.upload_buffer()