"""MapReduce framework Worker node."""
import heapq
import json
import logging
import socket
import threading
import time
import tempfile
import os
import shutil
import subprocess
import pathlib
import hashlib
import contextlib
from collections import defaultdict
import click


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.worker_pid = os.getpid()
        self.worker_host = host
        self.worker_port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.state = None
        self.lock = threading.Lock()
        self.shutdown_event = threading.Event()
        
        # Listen for incoming connections in a separate thread - TCP Thread
        self.listen_thread = threading.Thread(target=self.listen_for_messages)
        self.listen_thread.start()
        LOGGER.info("STARTING listen thread")

        # 2) Send the register message to the Manager.
        # Make sure you are listening before sending this message.
        self.send_register_message()

        # test
        self.listen_thread.join()
        LOGGER.info("JOINED listen thread to main thread")
        for thread in threading.enumerate():
            LOGGER.info(f"Thread: {thread.name} still active")


    def send_register_message(self):
        """Worker Registration [MW]: Send TCP register message to the Manager."""
        # The Manager will then respond with a message acknowledging the
        # Worker has registered:
        register_msg = {"message_type": "register",
                        "worker_host": self.worker_host,
                        "worker_port": self.worker_port
                        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((self.manager_host, self.manager_port))
            except ConnectionRefusedError:
                #return False
                LOGGER.error("Worker TCP Connection refused")
            message = json.dumps(register_msg)
            sock.sendall(message.encode('utf-8'))
            LOGGER.info("Sent register message to the Manager")

    
    def listen_for_messages(self):
        """Listen for incoming TCP messages from Manager."""
        heartbeat_thread = threading.Thread()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.worker_host, self.worker_port))
            sock.listen()
            LOGGER.info(f"Worker listening for TCP connections on {self.worker_host}: {self.worker_port}")
            LOGGER.info(f"Manager host: {self.manager_host}, Manager port: {self.manager_port}")
            while not self.shutdown_event.is_set():
                conn, addr = sock.accept()
                with conn:
                    LOGGER.info(f"Connected by {addr}")
                    data = conn.recv(1024)
                    LOGGER.info(f"______Received data: {data}")
                    if not data:
                        continue
                    try:
                        msg = json.loads(data.decode('utf-8'))
                        LOGGER.info(f"WorkerTCPListener: handle message {msg}")
                        """Receive and handle incoming messages from Manager"""
                        try:
                            LOGGER.info(f"Worker {self.worker_pid} received message: {msg.get("message_type")}")

                            if msg.get("message_type") == "register_ack":
                                LOGGER.info("Worker handling register_ack message")
                                # Upon receiving register_ack message, start heartbeat thread
                                heartbeat_thread = threading.Thread(target=self.send_heartbeat_messages)
                                LOGGER.info("STARTING heartbeat thread")
                                heartbeat_thread.start()
                                LOGGER.info("Worker heartbeat thread started")
                                self.state = "ready"

                            if msg.get("message_type") == "shutdown":
                                LOGGER.info("Worker handling shutdown message")
                                # If the Manager sends a shutdown message, the Worker should
                                # stop sending heartbeat messages and exit.
                                self.shutdown_event.set()
                                LOGGER.info("Worker shutdown event set")
                                self.status = "dead"
                                for thread in threading.enumerate():
                                        LOGGER.info(f"Thread: {thread.name} still active")
                                try:
                                    LOGGER.info("Attempting to join heartbeat thread with main thread")
                                    heartbeat_thread.join()
                                except Exception as e:
                                    LOGGER.info("Error joining heartbeat thread: {e}")
                                    pass
                                return # added 7:27pm
                            if msg.get("message_type") == "new_map_task":
                                LOGGER.info("Worker handling new_map_task message")
                                """
                                map_message = {
                                    "message_type": "new_map_task",
                                    "task_id": i,
                                    "input_paths": partitions[i],
                                    "executable": self.current_job["mapper_executable"],
                                    "output_directory": self.current_job["output_directory"],
                                    "num_partitions": self.current_job["num_reducers"],
                                }
                                """
                                # Run the map executable on the specified input files.
                                # Partition output of the map executable into a new temporary directory, local to the Worker.
                                # Sort each output file by line using UNIX sort.
                                # Move each sorted output file to the shared temporary directory specified by the Manager.

                                task_id = msg.get("task_id")
                                executable = msg.get("mapper_executable")
                                input_paths = msg.get("input_paths")
                                output_dir = msg.get("output_directory")

                                with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
                                    LOGGER.info(f"Created tmpdir for worker map task: {tmpdir}")
                                    output_files = [os.path.join(tmpdir, f"maptask{task_id:05d}-part{partition_number:05d}") for partition_number in range(msg.get("num_partitions"))]

                                    with contextlib.ExitStack() as stack:
                                        files = [stack.enter_context(open(filename, 'a', encoding="utf-8")) for filename in output_files]

                                        for input_file in input_paths:
                                            with open(input_file, 'r', encoding="utf-8") as infile:
                                                for line in infile:
                                                    key = line.split("\t")[0]
                                                    keyhash = int(hashlib.md5(key.encode("utf-8")).hexdigest(), base=16)
                                                    partition_number = keyhash % msg.get("num_partitions")
                                                    files[partition_number].write(line)

                                    # Execute the map executable
                                    for output_file in output_files:
                                        with open(output_file, 'r', encoding="utf-8") as infile:
                                            with subprocess.Popen([executable], stdin=infile, stdout=subprocess.PIPE, text=True) as map_process:
                                                sorted_output = map_process.stdout.read()

                                        # Write the sorted output to the output file
                                        with open(output_file, 'w', encoding="utf-8") as outfile:
                                            outfile.write(sorted_output)

                                    # Sort and move each intermediate file
                                    for filename in output_files:
                                        subprocess.run(["sort", "-o", filename, filename], check=True)
                                        LOGGER.info("Sorted %s", filename)
                                        shutil.move(filename, os.path.join(output_dir, os.path.basename(filename)))
                                        LOGGER.info("Moved %s", filename)

                                # Send finished message to Manager
                                finished_msg = {
                                    "message_type": "finished",
                                    "task_id": task_id,
                                    "worker_host": self.worker_host,
                                    "worker_port": self.worker_port
                                }
                                self.send_tcp_message(self.manager_host, self.manager_port, finished_msg)

                            if msg.get("message_type") == "new_reduce_task":
                                """Reduce job."""
                                with tempfile.TemporaryDirectory(
                                        prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
                                    LOGGER.info("Created tmpdir %s", tmpdir)
                                    with contextlib.ExitStack() as stack:
                                        files = [stack.enter_context(open(fname, encoding="utf-8"))
                                                for fname in msg.get("input_paths")]
                                        instream = heapq.merge(*files)
                                        filename = pathlib.PurePath(tmpdir, f"part-{task_id:05d}")
                                        with open(filename, 'a', encoding="utf-8") as outfile:
                                            with subprocess.Popen(
                                                [executable],
                                                text=True,
                                                stdin=subprocess.PIPE,
                                                stdout=outfile,
                                            ) as reduce_process:
                                                LOGGER.info("Executed %s", executable)
                                                # Pipe input to reduce_process
                                                for line in instream:
                                                    reduce_process.stdin.write(line)

                                    # move file to output folder
                                    for filename in os.listdir(pathlib.Path(tmpdir)):
                                        shutil.move(pathlib.Path(tmpdir, filename),
                                                    pathlib.Path(msg.get("output_directory"), filename))
                                        LOGGER.info("Moved %s", filename)

                                # Send finished message to Manager
                                finished_msg = {
                                    "message_type": "finished",
                                    "task_id": task_id,
                                    "worker_host": self.worker_host,
                                    "worker_port": self.worker_port
                                }
                                self.send_tcp_message(self.manager_host, self.manager_port, finished_msg)    
                        except Exception as e:
                            LOGGER.error(f"Error handling message: {e}")
                    except json.JSONDecodeError:
                        LOGGER.info(f"WorkerTCPListener: no msg")
                        # Ignore invalid messages
                        continue

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

    def send_heartbeat_messages(self):
        while not self.shutdown_event.is_set():
            try:
                if self.shutdown_event.is_set():
                    LOGGER.info("break triggered in send_heartbeat_messages")
                    break   
                self.send_heartbeat()
            except Exception as e:
                LOGGER.error(f"Error sending heartbeat: {e}")
            time.sleep(2)  # Sleep for 2 seconds between heartbeats
        # test


    def send_heartbeat(self):
        # Connect to manager and send heartbeat message
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            heartbeat_msg = {
                "message_type": "heartbeat",
                "worker_host": self.worker_host,
                "worker_port": self.worker_port,
            }
            sock.sendall(json.dumps(heartbeat_msg).encode())
            LOGGER.info("Sent heartbeat message to the Manager")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
