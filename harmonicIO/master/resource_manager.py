import queue

from harmonicIO.master.binpacking import BinPacking
from harmonicIO.general.definition import Definition
from harmonicIO.master.meta_table import LookUpTable

class Container():
    pass

class ContainerQueue():
    
    def __init__(self, queue_cap=0):
        self.__queue = queue.Queue(maxsize=queue_cap)
        
    def get_queue_length(self):
        return self.__queue.qsize()

    def is_container_in_queue(self, c_image_name):
        for item in self.__queue.queue:
            if item[Definition.Container.get_str_con_image_name] == c_image_name:
                return True
        return False
            
    def update_containers(self, c_image_name, update_data):
        for item in self.__queue.queue:
            if item[Definition.Container.get_str_con_image_name] == c_image_name:
                for field in update_data:
                    item[field] = update_data[field]

    def put_container(self, container_data):
        self.__queue.put(container_data)

    def get_current_queue_list(self):
        return list(self.__queue.queue)

    def get_next_container(self):
        self.__queue.get()

    def get_next_container_do_task(self, task, task_params):
        next_item = self.__queue.get()
        task((next_item, task_params))
        self.__queue.task_done()



class ContainerAllocator():

    @staticmethod
    def avg_wasted_space(c_list, layout):
        total_wasted_space = 0.0
        for worker in layout:
            for c_index in worker[1]:
                total_wasted_space += c_list[c_index]['avg_cpu']

        return total_wasted_space/len(c_list)


    def __init__(self, container_queue):
        self.container_queue = container_queue
        self.bins = []
        self.packing_algorithm = BinPacking.first_fit # TODO: make configurable
        
    def get_current_layout(self):
        # TODO: implement current layout
        return None

    def check_near_full(self):
        """
        checks if avg cpu consumption is above 80% of total available,
        and if number of used workers are equal to or 1 less than available workers
        """
        return avg_wasted_space(self.container_queue.get_current_queue_list()) > 0.8 and len(self.bins) > len(LookUpTable.Workers.__workers) -1




    def pack_containers_optimally(self, number_of_workers):
        container_list = self.container_queue.get_current_queue_list()
        layout = self.packing_algorithm(container_list)
        

        while self.avg_wasted_space(container_list, layout) < 0.9:
            # we are not using 90% of the total available cpu on average among all workers

            # Do bin packing with t fewer workers, where t is some tunable parameter
            pass



                





class WorkerProfiler():
    
    pass


class LoadPredictor():

    pass