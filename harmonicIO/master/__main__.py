from harmonicIO.general.services import SysOut
from .jobqueue import JobManager

"""
Master entry point
"""

def run_queue_manager(manager):
    """
    Run job queue manager thread
    can be several managers to manage large amount of queued jobs
    """
    import threading
    for i in range(manager.queuer_threads):
        manager_thread = threading.Thread(target=manager.job_queuer)
        manager_thread.daemon = True
        manager_thread.start()

    supervisor_thread = threading.Thread(target=manager.queue_supervisor)
    supervisor_thread.daemon = True
    supervisor_thread.start()

    SysOut.out_string("Job queue started")


def run_rest_service():
    """
    Run rest as in a thread function
    """
    from .rest_service import RESTService
    rest = RESTService()
    rest.run()


def run_msg_service():
    """
    Run msg service to eliminate back pressure
    """
    from .configuration import Setting
    from .server_socket import ThreadedTCPServer, ThreadedTCPRequestHandler
    import threading
    server = ThreadedTCPServer((Setting.get_node_addr(), Setting.get_data_port_start()),
                               ThreadedTCPRequestHandler, bind_and_activate=True)

    # Start a thread with the server -- that thread will then start one
    server_thread = threading.Thread(target=server.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True

    SysOut.out_string("Enable Messaging System on port: " + str(Setting.get_data_port_start()))

    server_thread.start()

    """ Have to test for graceful termination. """
    # server.shutdown()
    # server.server_close()


if __name__ == '__main__':
    """
    Entry point
    """
    SysOut.out_string("Running Harmonic Master")

    # Load configuration from file
    from .configuration import Setting
    Setting.read_cfg_from_file()

    # Print instance information
    SysOut.out_string("Node name: " + Setting.get_node_name())
    SysOut.out_string("Node address: " + Setting.get_node_addr())
    SysOut.out_string("Node port: " + str(Setting.get_node_port()))

    # Create thread for handling REST Service
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor()

    # Run messaging system service
    pool.submit(run_msg_service)

    # Binding commander to the rest service and enable REST service
    pool.submit(run_rest_service)
    
    # create a job manager which is a queue manager supervising the creation of containers, both via user and auto-scaling
    jobManager = JobManager(30, 100, 5, 1) # 30 seconds interval between checking, 100 requests in queue before increase, add 5 new containers, 1 thread for queue supervisor
    # Run job queue manager thread
    pool.submit(run_queue_manager, jobManager)
