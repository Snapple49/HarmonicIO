from harmonicIO.master.irm_components import ContainerAllocator, ContainerQueue, WorkerProfiler, LoadPredictor
from .binpacking import BinPacking
from .meta_table import LookUpTable

import time
import threading

class IntelligentResourceManager():
    container_manager = None

    @staticmethod
    def start_irm(packing_algorithm):
        IntelligentResourceManager.container_manager = ContainerAllocator(packing_algorithm)
        worker_scaling_thread = threading.Thread(target=IntelligentResourceManager.scale_workers)
        worker_scaling_thread.daemon = True
        worker_scaling_thread.start()

    @staticmethod
    def queue_container(container_data):
        IntelligentResourceManager.container_manager.container_q.put_container(container_data)
        
    @staticmethod
    def remove_container(c_name, csid):
        # called from metatable, update available containers accordingly
        IntelligentResourceManager.container_manager.remove_container_by_id(c_name, csid)

    @staticmethod
    def scale_workers():
        while True:
            time.sleep(1)
            current_workers = LookUpTable.Workers.active_workers() # the amount of workers currently available
            IntelligentResourceManager.container_manager.target_worker_number # the amount of workers we need
            while not current_workers == IntelligentResourceManager.container_manager.target_worker_number:
                time.sleep(1)
                print("We are not at target worker number! {} {}".format(current_workers, IntelligentResourceManager.container_manager.target_worker_number))
                if current_workers < IntelligentResourceManager.container_manager.target_worker_number:
                    # start more workers
                    print("Enabling worker, current active workers: {}".format(current_workers))
                    LookUpTable.Workers.enable_worker()
                else:
                    # disable workers
                    print("Disabling worker, current active workers: {}".format(current_workers))
                    LookUpTable.Workers.disable_worker()

        
