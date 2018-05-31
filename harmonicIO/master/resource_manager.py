from harmonicIO.master.irm_components import ContainerAllocator as ca, ContainerQueue as cq, WorkerProfiler as prof, LoadPredictor as lp
from harmonicIO.master.binpacking import BinPacking as bp
from harmonicIO.master.meta_table import LookUpTable as lut


class IntelligentResourceManager():

    container_manager = ca(bp.first_fit)
    #container_manager = None

    @staticmethod
    def start_irm(packing_algorithm):
        IntelligentResourceManager.container_manager = ca(packing_algorithm)

    @staticmethod
    def queue_container(container_data):
        IntelligentResourceManager.container_manager.container_q.put_container(container_data)
        
        
    @staticmethod
    def scale_workers(parameter_list):
        current_workers = len(lut.Workers.verbose()) # the amount of workers currently available
        #current_workers = len(IntelligentResourceManager.container_manager.bins)
        IntelligentResourceManager.container_manager.target_worker_number # the amount of workers we need
        
