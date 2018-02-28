"""
Worker entry point.
"""
import threading
import urllib3
from .configuration import Setting
from harmonicIO.general.services import SysOut, Services
from harmonicIO.general.definition import Definition, CRole
from .garbage_collector import GarbageCollector
import json


def run_rest_service():
    """
    Run rest as in a thread function
    """
    from .rest_service import RESTService
    rest = RESTService()
    rest.run()


def start_gc_thread():
    carbage_collector = GarbageCollector(10)
    gc_thread = threading.Thread(carbage_collector.collect_exited_containers())
    gc_thread.daemon = True
    gc_thread.start()

    SysOut.out_string("Garbage collector started")


def update_worker_status():
    """
    Update the worker status to the master as well as container info.
    """

    threading.Timer(5, update_worker_status).start()
    """
    Get machine status by calling a unix command and fetch for load average
    """

    content = Services.get_machine_status(Setting, CRole.WORKER)
    content[Definition.REST.get_str_docker()] = DockerService.get_containers_status()
    content[Definition.REST.get_str_local_imgs()] = DockerService.get_local_images()
    
    s_content = bytes(json.dumps(content), 'utf-8')

    html = urllib3.PoolManager()
    try:
        r = html.request('PUT', Definition.Master.get_str_check_master(Setting.get_master_addr(),
                                                                       Setting.get_master_port(),
                                                                       Setting.get_token()),
                         body=s_content)

        if r.status != 200:
            SysOut.err_string("Cannot update worker status to the master!")
        else:
            SysOut.debug_string("Reports status to master node complete.")

    except Exception as e:
        SysOut.err_string("Master is not available!")
        print(e)


if __name__ == "__main__":
    """
    Entry point
    """
    SysOut.out_string("Running Harmonic Worker")

    # Load configuration from file
    Setting.read_cfg_from_file()

    # Override master and repo address
    # Setting.set_variables_from_ev()

    # Print instance information
    SysOut.out_string("Node name: " + Setting.get_node_name())
    SysOut.out_string("Node internal address: " + Setting.get_node_internal_addr())
    if Setting.get_node_external_addr():
        SysOut.out_string("Node external address: " + Setting.get_node_external_addr())
    SysOut.out_string("Node port: " + str(Setting.get_node_port()))
    SysOut.out_string("Port range: {0} to {1} ({2} ports available)".format(Setting.get_data_port_start(),
                                                                 Setting.get_data_port_stop(),
                                                                 Setting.get_data_port_stop() -
                                                                 Setting.get_data_port_start()))

    # Init docker driver
    from .docker_service import DockerService
    DockerService.init()

    # Create thread for handling REST Service
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor()

    # Binding commander to the rest service and enable REST service
    pool.submit(run_rest_service)

    # Update the worker status
    pool.submit(update_worker_status)

    # Start garbage collector thread
    pool.submit(start_gc_thread)