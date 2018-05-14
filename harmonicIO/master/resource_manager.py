import math
import queue
import threading
import json
import time
from urllib.request import urlopen

#from binpacking import BinPacking, Bin
from harmonicIO.master.binpacking import BinPacking, Bin
from harmonicIO.general.definition import Definition
from harmonicIO.master.meta_table import LookUpTable

class Container():
    pass

class ContainerQueue():
    
    def __init__(self, queue_cap=0):
        self.__queue = queue.Queue(maxsize=queue_cap)
        self.container_queue_lock = threading.Lock()
        
    def get_queue_length(self):
        return self.__queue.qsize()

    def is_container_in_queue(self, c_image_name):
        for item in self.__queue.queue:
            if item['c_image_name'] == c_image_name:
                return True
        return False
            
    def update_containers(self, update_data):
        self.container_queue_lock.acquire()
        try:
            for item in self.__queue.queue:
                if item['c_name'] == update_data['c_name']:
                    for field in update_data:
                        item[field] = update_data[field]
        finally:
            self.container_queue_lock.release()


    def put_container(self, container_data):
        self.container_queue_lock.acquire()
        self.__queue.put(container_data)
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

    def queue_manager(self):
        while True:
            try:
                self.allocation_lock.acquire()
                container = self.allocation_q.get()
                
                for worker in LookUpTable.Workers.__workers:
                    if worker["bin_index"] == container["bin_index"]:
                        target_worker = (worker[Definition.get_str_node_addr()], worker[Definition.get_str_node_port()])

                if target_worker:
                    try:
                        sid = self.start_container_on_worker(target_worker, container)
                        container["bin_status"] = Bin.ContainerBinStatus.RUNNING
                    except Exception as e:
                        print(e)
                
                container[Definition.Container.Status.get_str_sid()] = sid

            finally:
                self.allocation_q.task_done()
                self.allocation_lock.release()



    def __init__(self, cq, packing_algo):
        self.container_q = cq
        self.packing_algorithm = packing_algo # TODO: make configurable
        self.allocation_q = queue.Queue()
        self.allocation_lock = threading.Lock()
        self.bins = []
        self.bin_layout_lock = threading.Lock()

        for _ in range(4):            
            queue_manager_thread = threading.Thread(target=self.queue_manager)
            queue_manager_thread.daemon=False
            queue_manager_thread.start()



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

    def pack_containers(self, number_of_current_workers):
        
        self.bin_layout_lock.acquire() # bin layout may not be mutated extrenally during packing
        try:
            container_list = self.container_q.get_current_queue_list()
            bins_layout = self.packing_algorithm(container_list, self.bins)
            self.bins = bins_layout
        finally:
            self.bin_layout_lock.release()

        minimum_worker_number = len(bins_layout)
        
        for bin_ in bins_layout:
            for item in bin_.items:
                if item["bin_status"] == Bin.ContainerBinStatus.PACKED:
                    self.allocation_q.put(item)
                    item["bin_status"] = Bin.ContainerBinStatus.QUEUED


        self.target_worker_number = minimum_worker_number + self.calculate_overhead_workers(number_of_current_workers)

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
                bin_.update_items_in_bin("c_image_name", update_data)
        finally:
            self.bin_layout_lock.release()

    def update_queued_containers(self, update_data):
        self.allocation_lock.acquire()
        try:
            for item in self.allocation_q.queue:
                if item['c_name'] == update_data['c_name']:
                    for field in update_data:
                        item[field] = update_data[field]
        finally:
            self.allocation_lock.release()

    def remove_container_from_bin(self, target_container, target_bin):
        """
        remove a container of the provided container image name from the specified bin. Should only be called externally
        and when a container finishes and exits the system
        """
        self.bin_layout_lock.acquire()
        try:
            self.bins[target_bin].remove_item_from_bin(target_container)
        finally:
            self.bin_layout_lock.release()

    def start_container_on_worker(self, target_worker, container):
        # send request to worker
        worker_url = "http://{}:{}/docker?token=None&command=create".format(target_worker[0], target_worker[1])
        req_data = bytes(json.dumps(container), 'utf-8') 
        resp = urlopen(worker_url, req_data) # NOTE: might need increase in timeout to allow download of large container images!!!

        if resp.getcode() == 200: # container was created
            sid = str(resp.read(), 'utf-8')
            print("Received sid from container: " + sid)
            return sid
        return False





class WorkerProfiler():
    
    def __init__(self, cq, ca, interval):
        self.c_queue = cq
        self.c_allocator = ca
        self.update_interval = interval
        self.update_fields = [Definition.Container.get_str_container_os, "bin_status", "bin_index", "avg_cpu"]

    def update_container_information(self):
        while True:
            time.sleep(self.update_interval)

            # update all containers in both container and allocation queues (as they are waiting) and in the bins with new data
            # from the meta table

            for container in LookUpTable.ImageMetadata.verbose():
                
                # container queue
                self.c_queue.update_containers(container)
                    
                # allocation queue
                self.c_allocator.update_queued_containers(container)

                # bins
                self.c_allocator.update_binned_containers(container)

    def gather_container_metadata(self):
        """
        transfers data from individual containers in metadata to container image-based metadata which is more interesting
        for the resurce manager, such as average cpu usage across all instances of a specific container image.
        """

        running_containers = LookUpTable.Containers.verbose()
        current_workers = LookUpTable.Workers.verbose()
        for container_name in running_containers:
            instance_counter = 0
            for worker in current_workers:
                for instance in 

                    
    
        

class LoadPredictor():
    pass



