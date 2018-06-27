from harmonicIO.master.irm_components import ContainerAllocator as ca, ContainerQueue as cq, WorkerProfiler as prof, LoadPredictor as lp
from harmonicIO.master.binpacking import BinPacking as bp
from harmonicIO.master.meta_table import LookUpTable as lut

import time
import threading

class IntelligentResourceManager():
    __container_manager = None

    @staticmethod
    def start_irm(packing_algorithm):
        IntelligentResourceManager.__container_manager = ca(packing_algorithm)
        worker_scaling_thread = threading.Thread(target=IntelligentResourceManager.scale_workers)
        worker_scaling_thread.daemon = True
        worker_scaling_thread.start()

    @staticmethod
    def queue_container(container_data):
        IntelligentResourceManager.__container_manager.container_q.put_container(container_data)
        
    @staticmethod
    def remove_container(c_name, csid):
        # called from metatable, update available containers accordingly
        IntelligentResourceManager.__container_manager.remove_container_by_id(c_name, csid)

    @staticmethod
    def scale_workers():
        while True:
            time.sleep(1)
            current_workers = lut.Workers.active_workers() # the amount of workers currently available
            IntelligentResourceManager.__container_manager.target_worker_number # the amount of workers we need
            while not current_workers == IntelligentResourceManager.__container_manager.target_worker_number:
                if current_workers < IntelligentResourceManager.__container_manager.target_worker_number:
                    # start more workers
                    lut.Workers.enable_worker()
                else:
                    # disable workers
                    lut.Workers.disable_worker()

        
