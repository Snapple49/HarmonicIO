import math
import queue
import threading
import json
import time
from urllib.request import urlopen
import copy

from harmonicIO.general.services import SysOut
from harmonicIO.general.definition import Definition
from .binpacking import BinPacking, Bin
from .meta_table import LookUpTable
from .messaging_system import MessagesQueue
from .configuration import Setting, IRMSetting

BinStatus = Bin.ContainerBinStatus

class ContainerQueue():
    
    def __init__(self, queue_cap=0):
        self.__queue = queue.Queue(maxsize=queue_cap)
        self.container_queue_lock = threading.Lock()
        
    def get_queue_length(self):
        return self.__queue.qsize()

    def is_container_in_queue(self, c_image):
        for item in self.__queue.queue:
            if item[Definition.Container.get_str_con_image_name()] == c_image:
                return True
        return False

    def view_queue(self):
        return self.__queue.queue
            
    def update_containers(self, c_image, update_data):
        self.container_queue_lock.acquire()
        try:
            for item in self.__queue.queue:
                if item[Definition.Container.get_str_con_image_name()] == c_image:
                    for field in update_data:
                        item[field] = update_data[field]
        finally:
            self.container_queue_lock.release()


    def put_container(self, container_data):
        self.container_queue_lock.acquire()
        try:
            self.__queue.put(container_data)
        finally:
            self.container_queue_lock.release()

    def get_current_queue_list(self):
        """
        dequeue all items currently available in queue and put into a list
        """
        current_list = []
        self.container_queue_lock.acquire()
        try:
            for _ in range(len(self.__queue.queue)):
                current_list.append(self.__queue.get())
        finally:
            self.container_queue_lock.release()
        return current_list 


class ContainerAllocator():

    def __init__(self, packing_algo):
        self.target_worker_number = 0
        self.container_q = ContainerQueue()
        self.packing_algorithm = packing_algo 
        self.allocation_q = queue.Queue()
        self.allocation_lock = threading.Lock()
        self.bins = []
        self.bin_layout_lock = threading.Lock()
        self.size_descriptor = "avg_cpu"

        config = IRMSetting()
        self.packing_interval = config.packing_interval
        self.default_cpu_share = config.default_cpu_share
        self.profiler = WorkerProfiler(self.container_q, self, config.profiling_interval)
        self.load_predictor = LoadPredictor(self, config.step_length,config.roc_lower, config.roc_upper,
                                            config.roc_minimum, config.queue_limit, config.waiting_time,
                                            config.large_increment, config.small_increment)

        print("Settings loaded!")

        # start threads that manage packing and queues
        bin_packing_manager = threading.Thread(target=self.packing_manager)
        bin_packing_manager.daemon=True
        bin_packing_manager.start()

        for _ in range(4):            
            queue_manager_thread = threading.Thread(target=self.queue_manager)
            queue_manager_thread.daemon=True
            queue_manager_thread.start()

    def queue_manager(self):
        SysOut.debug_string("Started a queue manager thread! ID: {}".format(threading.current_thread()))
        deleteflag = "deleteme"
        while True:
            try:
                time.sleep(1)
                target_worker = None
                sid = None
                self.allocation_lock.acquire()
                container_data = self.allocation_q.get().data
                workers = LookUpTable.Workers.verbose()
                
                if container_data["bin_status"] in [BinStatus.PACKED, BinStatus.QUEUED]:
                    
                    for worker in workers:
                        if workers[worker].get("bin_index", -99) == container_data["bin_index"]:
                            target_worker = (workers[worker][Definition.get_str_node_addr()], workers[worker][Definition.get_str_node_port()])

                    if target_worker:
                        try:
                            sid = self.start_container_on_worker(target_worker, container_data)
                        except Exception as e:
                            SysOut.debug_string(e)
                    
                    if sid and not sid == deleteflag:
                        container_data[Definition.Container.Status.get_str_sid()] = sid
                        container_data["bin_status"] = BinStatus.RUNNING
                        SysOut.debug_string("Added container with sid {}".format(sid))

                    else:
                        SysOut.debug_string("Could not start container on target worker! Requeueing as failed!\n")
                        container_data[Definition.Container.Status.get_str_sid()] = deleteflag
                        
                        # ensure fresh copy of data is requeued and remove item from bins
                        new_container_data = copy.deepcopy(container_data)
                        self.bins[container_data["bin_index"]].remove_item_in_bin(Definition.Container.Status.get_str_sid(), deleteflag)
                        for field in ["bin_index", "bin_status", Definition.Container.Status.get_str_sid()]:
                            del new_container_data[field]

                        SysOut.debug_string("Requeueing new container: {}".format(new_container_data))
                        # requeue the copy
                        self.container_q.put_container(new_container_data)

            finally:
                self.allocation_q.task_done()
                self.allocation_lock.release()

    def packing_manager(self):
        SysOut.debug_string("Started bin packing manager! ID: {}".format(threading.current_thread()))
        while True:
            time.sleep(self.packing_interval)
            self.pack_containers()
            self.update_bins()

    def average_wasted_space(self, bins):
        total_wasted_space = 0.0
        for bin_ in bins:
            total_wasted_space += bin_.free_space
        return total_wasted_space/len(bins)

    def acceptable_wasted_space(self, bins):
        """
        check that each bin except the last has at most 10% wasted space
        """
        for bin_ in bins[:-1]:
            if bin_.free_space < 0.9:
                return False
        return True

    def pack_containers(self):
        """
        perform bin packing with containers in workers, giving each container a worker to be allocated on and the amount of workers
        """
        self.bin_layout_lock.acquire() # bin layout may not be mutated externally during packing
        try:
            container_list = self.container_q.get_current_queue_list()
            # if any containers don't yet have average cpu usage, add default value now
            for cont in container_list:
                if cont.get(self.size_descriptor, None) == None:
                    cont[self.size_descriptor] = self.default_cpu_share * 0.01
            bins_layout = self.packing_algorithm(container_list, self.bins, self.size_descriptor)
            self.bins = bins_layout
        
            for bin_ in bins_layout:
                for item in bin_.items:
                    if item.data["bin_status"] == BinStatus.PACKED:
                        item.data["bin_status"] = BinStatus.QUEUED
                        self.allocation_q.put(item)
        
        finally:
            self.bin_layout_lock.release()

        self.target_worker_number = len(bins_layout) + self.calculate_overhead_workers(LookUpTable.Workers.active_workers())

    def update_bins(self):
        self.bin_layout_lock.acquire()
        indices = []
        try:
            for i in range(len(self.bins)):
                if not self.bins[i].items:
                    indices.insert(0, i)
            for i in indices:
                del self.bins[i]
        finally:
            self.bin_layout_lock.release()

    def calculate_overhead_workers(self, number_of_current_workers):
        """
        calculates a suggested amount of additional workers to have some headroom with available workers, based on some logarithmic proportion when above 10 workers
        """
        if number_of_current_workers < 10:
            return 1
        elif number_of_current_workers < 100:
            return math.ceil(math.log(number_of_current_workers)*0.5)
        else:
            return math.trunc(math.log(number_of_current_workers))

    def __enqueue_container(self, container):
        self.allocation_q.put(container)

    def update_binned_containers(self, update_data):
        """
        update all containers of the same image as in update_data within all bins
        """
        self.bin_layout_lock.acquire()
        try:
            for bin_ in self.bins:
                bin_.update_items_in_bin(Definition.Container.get_str_con_image_name(), update_data)
        finally:
            self.bin_layout_lock.release()

    def update_queued_containers(self, c_name, update_data):
        self.allocation_lock.acquire()
        try:
            for item in self.allocation_q.queue:
                if item[Definition.Container.get_str_con_image_name()] == c_name:
                    for field in update_data:
                        item[field] = update_data[field]
        finally:
            self.allocation_lock.release()

    def remove_container_by_id(self, csid):
        """
        remove container with specified short_id. Should only be called via external event when a container finishes and exits the system
        or when container could not be allocated after being packed and queued
        """
        target_bin = -1
        self.bin_layout_lock.acquire()
        try:
            for _bin in self.bins:
                for item in _bin.items:
                    if item.data.get(Definition.Container.Status.get_str_sid(), "") == csid:
                        target_bin = _bin.index
                        break

            if target_bin > -1:
                self.bins[target_bin].remove_item_in_bin(Definition.Container.Status.get_str_sid(), csid)

        finally:
            self.bin_layout_lock.release()
            return True if target_bin > -1 else False

    def start_container_on_worker(self, target_worker, container):
        # send request to worker
        cont = dict(container)
        cont[Definition.Container.get_str_cpu_share()] = container[self.size_descriptor]
        worker_url = "http://{}:{}/docker?token=None&command=create".format(target_worker[0], target_worker[1])
        req_data = bytes(json.dumps(cont), 'utf-8') 
        resp = urlopen(worker_url, req_data) 

        if resp.getcode() == 200: # container was created
            sid = str(resp.read(), 'utf-8')
            print("Received sid from container: " + sid)
            return sid
        return None




class WorkerProfiler():
    
    def __init__(self, cq, ca, interval):
        self.c_queue = cq
        self.c_allocator = ca
        self.update_interval = interval
        self.update_fields = [Definition.Container.get_str_container_os, "bin_status", "bin_index", ca.size_descriptor]

        self.updater_thread = threading.Thread(target=self.update_container_information)
        self.updater_thread.daemon=True
        self.updater_thread.start()

    def update_container_information(self):
        SysOut.debug_string("Started a profiler thread! ID: {}".format(threading.current_thread()))
        while True:
            time.sleep(self.update_interval)

            # gather metadata about containers and put in meta table
            SysOut.debug_string("Profiling container metadata")
            self.gather_container_metadata()

            # update all containers in both container and allocation queues (as they are waiting) and in the bins with new data from the meta table
            for container_image in LookUpTable.ImageMetadata.verbose():
                container_data = LookUpTable.ImageMetadata.verbose()[container_image]
                container_data[Definition.Container.get_str_con_image_name()] = container_image

                # container queue
                self.c_queue.update_containers(container_image, container_data)
                SysOut.debug_string("Updated container queue")
                    
                # allocation queue
                self.c_allocator.update_queued_containers(container_image, container_data)
                SysOut.debug_string("Updated allocation queue")

                # bins
                self.c_allocator.update_binned_containers(container_data)
                SysOut.debug_string("Updated bins")
    
    def gather_container_metadata(self):
        """
        transfers data from individual containers in metadata to container image-based metadata which is more interesting
        for the resource manager, such as average cpu usage across all instances of a specific container image.
        """
        running_containers = LookUpTable.Containers.running_containers()
        current_workers = LookUpTable.Workers.verbose()
        SysOut.debug_string("Gathering metadata for containers: {}".format(running_containers))
        for container_name in running_containers:
            total_counter = 0
            avg_sum = 0
            for worker in current_workers:
                local_counter = 0
                if container_name in current_workers[worker]["local_image_stats"]:
                    for local in current_workers[worker][Definition.REST.get_str_docker()]:
                        if local[Definition.Container.Status.get_str_image()] == container_name:
                            local_counter +=1
                    avg_sum += current_workers[worker]["local_image_stats"].get(container_name, 0)["avg_cpu"] * local_counter
                total_counter += local_counter
            if total_counter:
                SysOut.debug_string("Pushing metadata: sum {} population {}".format(avg_sum, total_counter))
                LookUpTable.ImageMetadata.push_metadata(container_name, {self.c_allocator.size_descriptor : avg_sum/total_counter})

        # CURRENTLY DOING:
        # issue: only pushes first time

class LoadPredictor():
    
    def __init__(self, cm, step, lower, upper, minimum, q_size_limit, waiting_time, large, small):
        self.c_manager = cm
        self.step_length = step
        self.previous_total = 0
        self.image_data = {}

        self.roc_positive_lower = lower
        self.roc_positive_upper = upper
        self.roc_minimum = minimum
        self.queue_length_limit = q_size_limit
        self.wait_time = waiting_time
        self.large_increment = large
        self.small_increment = small

        self.analyzer_thread = threading.Thread(target=self.analyze_roc_for_autoscaling)
        self.analyzer_thread.daemon = False
        self.analyzer_thread.start()

    def calculate_messagequeue_total_roc(self):
        items_in_queue = 0
        current_queue = MessagesQueue.verbose()
        for name in current_queue:
            items_in_queue += current_queue[name]

        self.roc_total = (items_in_queue - self.previous_total) / self.step_length
        self.previous_total = items_in_queue
        return self.roc_total

    def calculate_roc_per_image(self):
        current_queue = MessagesQueue.verbose()
        for image in current_queue:
            if not self.image_data.get(image):
                self.image_data[image] = {"roc" : current_queue[image]}
            else:
                self.image_data[image]["roc"] = int((current_queue[image] - self.image_data[image]["roc"]) / self.step_length)
                
    def queue_container(self, container_data):
        self.c_manager.container_q.put_container(container_data)

    def analyze_roc_for_autoscaling(self):
        SysOut.debug_string("Started a load predictor thread! ID: {}".format(threading.current_thread()))
        while True:
            time.sleep(self.step_length)
            self.calculate_roc_per_image()

            for image in self.image_data:

                last_start = self.image_data[image].get("last_start", 0)
                if int(time.time()) - last_start > self.wait_time:

                    # a new container was not recently started so action should be taken if needed
                    roc = self.image_data[image]["roc"]
                    increment = 0
                    image_queue_length = MessagesQueue.verbose().get(image, 0)

                    # decide how many containers should be added, if any, and send these to the container queue
                    # cases: RoC above either limit or RoC negative but queue length above limit
                    if roc > self.roc_positive_upper:
                        increment = self.large_increment
                    elif roc > self.roc_positive_lower:
                        increment = self.small_increment
                    elif image_queue_length > self.queue_length_limit and roc > self.roc_minimum:
                        increment = self.large_increment
                    elif image_queue_length > self.queue_length_limit:
                        increment = self.small_increment

                    # NOTE: design decision, should created from autoscaling containers be volatile or not?
                    if increment > 0:
                        for _ in range(increment):
                            self.queue_container({Definition.Container.get_str_con_image_name() : image, "volatile" : True})
                        self.image_data[image]["last_start"] = int(time.time()) 


