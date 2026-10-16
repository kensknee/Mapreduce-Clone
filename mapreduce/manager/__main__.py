"""MapReduce framework Manager node."""
import json
import logging
import socket
import threading
import time
import tempfile
import os
import pathlib
import shutil
from queue import Queue
from collections import defaultdict
import click

# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    def __init__(self, host, port):
        LOGGER.info(f"Starting manager on {host}:{port}")
        self.host = host
        self.port = port
        self.workers = {}
        self.registered_order = []
        self.job_queue = Queue()
        self.current_job = None
        self.current_tasks = {}
        self.lock = threading.Lock()
        self.shutdown_event = threading.Event()

        self.udp_thread = threading.Thread(target=self.udp_listener)
        self.udp_thread.start()
        self.tcp_thread = threading.Thread(target=self.tcp_listener)
        self.tcp_thread.start()


        #self.timeout_thread = threading.Thread(target=self.check_timeouts)
        #self.timeout_thread.start()
        for thread in threading.enumerate():
            LOGGER.info(f"Thread: {thread.name} still active")
        self.udp_thread.join()
        LOGGER.info("UDP thread completed")
        self.tcp_thread.join()
        LOGGER.info("TCP thread completed")


        #self.timeout_thread.join()
        #LOGGER.info("Timeout thread completed")
        # for thread in threading.enumerate():
        #     LOGGER.info(f"Thread: {thread.name} still active")


    def tcp_listener(self):
        """Listen for incoming TCP messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.settimeout(1) # added
            s.listen()
            LOGGER.info(f"Manager listening for TCP connections on {self.host}:{self.port}")
            while not self.shutdown_event.is_set():
                time.sleep(0.1) # 7:52am
                try:
                    conn, addr = s.accept()
                except socket.timeout:
                    continue

                LOGGER.info(f"Connected by {addr}")
                data = conn.recv(1024)
                LOGGER.info(f"TCPL Received data: {data}")
                if not data:
                    LOGGER.info("No data received")
                    continue
                try:
                    msg = json.loads(data.decode('utf-8'))
                    self.handle_message(msg)
                except json.JSONDecodeError:
                    continue
                #break # added 8:50am

    def udp_listener(self):
        """Listen for incoming UDP heartbeat messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, self.port))
            LOGGER.info(f"Manager listening for UDP connections on {self.host}:{self.port}")
            while not self.shutdown_event.is_set():
                time.sleep(0.1) # 7:52am
                data = s.recv(1024)
                if not data:
                    LOGGER.info("Received empty UDP heartbeat message")
                    continue
                LOGGER.info(f"Received UDP heartbeat message: {data}")
                try:
                    msg = json.loads(data.decode('utf-8'))
                    if msg.get("message_type") == "heartbeat":
                        """Handle heartbeat messages from workers."""
                        LOGGER.info("In Manager handle_heartbeat")
                        worker_host = msg.get("worker_host")
                        worker_port = msg.get("worker_port")
                        worker_id = (worker_host, worker_port)
                        #with self.lock:
                        if worker_id in self.workers:
                            self.workers[worker_id]['last_heartbeat'] = time.time()
                    else:
                        LOGGER.info("Received UDP message is not heartbeat")
                except json.JSONDecodeError:
                    continue
                break # added 8:50am


    def handle_message(self, msg):
        """Handle incoming TCP messages."""
        LOGGER.info(f"handle_message: Received message: {msg}")
        msg_type = msg.get("message_type")
        if msg_type == "shutdown":
            self.handle_shutdown()
        elif msg_type == "new_manager_job":
            self.handle_new_job(msg)
        elif msg_type == "register":
            self.handle_register(msg)


    def handle_register(self, msg):
        """Handle worker registration message."""
        LOGGER.info("In Manager handle_register")
        worker_host = msg["worker_host"]
        worker_port = msg["worker_port"]
        worker_id = (worker_host, worker_port)
        message_dict = {
            "message_type": "register_ack",
            "worker_host": worker_host,
            "worker_port": worker_port,
        }
        #with self.lock:
        LOGGER.info(f"Worker {worker_id} registered. Added to workers list and registered order.")
        self.workers[worker_id] = {"status": "ready", "last_heartbeat": time.time()}
        self.registered_order.append(worker_id)
        if not self.send_tcp_message(worker_host, worker_port, message_dict):
            LOGGER.warning(f"Failed to send register_ack to worker {worker_id}")


    def handle_shutdown(self):
        """Handle shutdown message."""
        LOGGER.info("In Manager handle_shutdown")
        LOGGER.info("Shutting down Manager")
        self.shutdown_event.set()
        # with self.lock:
        for worker_id in self.workers:
            self.send_tcp_message(worker_id[0], worker_id[1], {"message_type": "shutdown"})
            LOGGER.info(f"Sent shutdown message to worker {worker_id}")

        LOGGER.info("Manager has shut down")


    def handle_new_job(self, msg):
        """New Job Request [Manager]."""
        LOGGER.info(f"Received new job: {msg}")
        job_id = self.job_queue.qsize()

        job_info = {
            "job_id": job_id,
            "input_directory": msg["input_directory"],
            "output_directory": msg["output_directory"],
            "mapper_executable": msg["mapper_executable"],
            "reducer_executable": msg["reducer_executable"],
            "num_mappers": msg["num_mappers"],
            "num_reducers": msg["num_reducers"],
        }
        # LOGGER.info(f"Job info: {job_info}")
        self.job_queue.put(job_info)
        LOGGER.info(f"Job Queue now has {self.job_queue.qsize()} jobs.")
        self.run_job(job_info)


    def run_job(self, job_info):
        """Manage job directory stuff."""
        output_directory = pathlib.Path(job_info["output_directory"])
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)
            LOGGER.info(f"Removed existing output directory: {output_directory}")
        os.makedirs(output_directory)
        LOGGER.info(f"Created output directory for job {job_info['job_id']}: {output_directory}")

        prefix = f"mapreduce-shared-job{job_info['job_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info(f"Created tmpdir for job {job_info['job_id']}: {tmpdir}")
            # FIXME: Change this loop so that it runs either until shutdown 
            # or when the job is completed.
            while not self.shutdown_event.is_set() and not self.current_job:
                # used to be 'or'
                time.sleep(0.1)
                if self.job_queue.empty():
                    # No running job and no job in queue
                    continue
                # No running job but job in queue
                self.current_job = self.job_queue.get()
                # Process Job here:
                self.input_partition()
                self.check_map()

            #self.assign_tasks(files, job_info, tmpdir)


    def input_partition(self):
        LOGGER.info(f"Current job: {self.current_job}")
        # Sort input files by name
        sorted_inputs = sorted(os.listdir(self.current_job["input_directory"]))
        # Divide files into num_mappers partitions using round-robin
        # defaultdict(list) example: {0: [file1, file3], 1: [file2]}
        partitions = defaultdict(list)
        for i, input_file in enumerate(sorted_inputs):
            task_id = i % self.current_job["num_mappers"]
            partitions[task_id].append(input_file)
        LOGGER.info(f"Partitions: {partitions}")
        # Allocate tasks to workers based on registration order
        # Ex: first worker 0 -> task_id 0
        #     second worker 1 -> task_id 1
        LOGGER.info(f"Registered order: {self.registered_order}")
        for i, worker_id in enumerate(self.registered_order):
            if (self.workers[worker_id]["status"] == "ready"):
                # Update worker status to busy
                self.workers[worker_id]["status"] = "busy"
                map_message = {
                    "message_type": "new_map_task",
                    "task_id": i,
                    "input_paths": partitions[i],
                    "executable": self.current_job["mapper_executable"],
                    "output_directory": self.current_job["output_directory"],
                    "num_partitions": self.current_job["num_reducers"],
                }
                LOGGER.info(f"Sending map message: {map_message}")
                self.send_tcp_message(worker_id[0], worker_id[1], map_message)


    def check_map(self):
        pass

    def send_tcp_message(self, host, port, message_dict):
        """Send customized tcp message."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((host, port))
            except ConnectionRefusedError:
                return False
            message = json.dumps(message_dict)
            sock.sendall(message.encode('utf-8'))
            return True


    def check_map_completion(self, job_info):
        """Check if all map tasks are completed and assign reduce tasks."""
        map_tasks_completed = all(
            task_id.startswith("map") and task_id not in self.current_tasks
            for task_id in self.current_tasks
        )
        if map_tasks_completed:
            reduce_task_id = f"reduce-{job_info['job_id']}-0"
            self.current_tasks[reduce_task_id] = job_info
            input_files = [
                os.path.join(job_info["tmpdir"], f"maptask{map_task_id:05d}-part{partition:05d}")
                for partition in range(job_info["num_reducers"])
                for map_task_id in range(job_info["num_mappers"])
            ]
            for worker_id, worker in self.workers.items():
                if worker["status"] == "ready":
                    worker["status"] = "busy"
                    self.send_tcp_message(worker_id[0], worker_id[1], {
                        "message_type": "new_reduce_task",
                        "task_id": 0,
                        "executable": job_info["reducer_executable"],
                        "input_paths": input_files,
                        "output_directory": job_info["output_directory"],
                    })
                    break


    # def check_timeouts(self):
    #     """Check for worker timeouts and reassign tasks if necessary."""
    #     LOGGER.info("In Manager check_timeouts")
    #     while not self.shutdown_event.is_set():
    #         time.sleep(0.1)
    #         current_time = time.time()
    #         #with self.lock:
    #         for worker_id, worker in list(self.workers.items()):
    #             LOGGER.info(f"Checking worker {worker_id}")
    #             if current_time - worker['last_heartbeat'] > 10:
    #                 LOGGER.warning(f"Worker {worker_id} timed out")
    #                 worker['status'] = 'dead'
    #                 #self.reassign_tasks(worker_id)
    #         if self.shutdown_event.is_set():
    #             break # added 8:50am

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
        )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)

if __name__ == "__main__":
    main()
