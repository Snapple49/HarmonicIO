import math
import queue
import threading
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
        for item in self.__queue.queue:
            if item['c_image_name'] == update_data['c_image_name']:
                for field in update_data:
                    item[field] = update_data[field]

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

    def __init__(self, container_queue):
        self.cq = container_queue
        self.allocation_queue = queue.Queue()
        self.bins = []
        self.packing_algorithm = BinPacking.first_fit # TODO: make configurable
        self.bin_layout_lock = threading.Lock()



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
        
    def check_near_full(self):
        """
        checks if avg cpu consumption is above 80% of total available,
        and if number of used workers are equal to or 1 less than available workers
        """
        return (self.average_wasted_space(self.bins) > 0.8 and len(self.bins))# > len(LookUpTable.Workers.__workers) -1)

    def pack_containers(self, number_of_current_workers):
        
        self.bin_layout_lock.acquire() # bin layout may not be mutated extrenally during packing
        try:
            container_list = self.cq.get_current_queue_list()
            bins_layout = self.packing_algorithm(container_list, self.bins)
            self.bins = bins_layout
        finally:
            self.bin_layout_lock.release()

        minimum_worker_number = len(bins_layout)
        
        for item in bins_layout:
            self.allocation_queue.put(item)
            
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

    def enqueue_container(self, container):
        self.allocation_queue.put(container)

    def update_binned_containers(self, update_data):
        """
        update all containers of the same image as in update_data within all bins
        """
        self.bin_layout_lock.acquire()
        try:
            for bin_ in self.bins:
                bin_.update_items_in_bin(Definition.Container.get_str_con_image_name, update_data)
        finally:
            self.bin_layout_lock.release()

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

    def allocate_containers(self):
        pass






class WorkerProfiler():
    
    pass


class LoadPredictor():

    pass