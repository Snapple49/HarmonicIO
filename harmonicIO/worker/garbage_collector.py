from .docker_service import DockerService
from harmonicIO.general.definition import Definition
from harmonicIO.general.services import SysOut

from time import sleep
class GarbageCollector():

    # interval between garbage collections in seconds
    gc_run_interval = 300

    def __init__(self, run_interval=300):
        self.gc_run_interval = run_interval


    def collect_exited_containers(self):
        while True:
            sleep(self.gc_run_interval)
            
            exited_containers = []
            current_containers = DockerService.get_containers_status()
            for cont in current_containers:
                # find exited containers
                if cont.get(Definition.Container.Status.get_str_status()) == 'exited':
                    exited_containers.append(cont.get(Definition.Container.Status.get_str_sid()))
                
            for sid in exited_containers:
                if not DockerService.delete_container(sid):
                    SysOut.debug_string("Could not delete target container: {}".format(sid))
            