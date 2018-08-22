from .irm_components import ContainerAllocator, ContainerQueue, WorkerProfiler, LoadPredictor
from .binpacking import BinPacking
from .meta_table import LookUpTable
from harmonicIO.general.services import SysOut

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
    def remove_container(csid):
        # called from metatable, update available containers accordingly
        IntelligentResourceManager.container_manager.remove_container_by_id(csid)

    @staticmethod
    def scale_workers():
        while True:
            time.sleep(1)
            current_workers = LookUpTable.Workers.active_workers() # the amount of workers currently available
            target_workers = IntelligentResourceManager.container_manager.target_worker_number # the amount of workers we need
            while not current_workers == target_workers:
                time.sleep(1)
                SysOut.debug_string("We are not at target worker number! Current: {} Target: {}".format(current_workers, target_workers))
                if current_workers < target_workers:
                    # start more workers
                    LookUpTable.Workers.enable_worker()
                else:
                    # disable workers
                    LookUpTable.Workers.disable_worker()
                current_workers = LookUpTable.Workers.active_workers()
                target_workers = IntelligentResourceManager.container_manager.target_worker_number
        
# CURRENTLY DOING
# ISSUE: does not scale down target number!?