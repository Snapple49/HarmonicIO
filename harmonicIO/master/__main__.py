from harmonicIO.general.services import SysOut
from .resource_manager import IntelligentResourceManager
from .binpacking import BinPacking
from .meta_table import LookUpTable

from .configuration import Setting
from .server_socket import ThreadedTCPServer, ThreadedTCPRequestHandler
from .rest_service import RESTService

import threading
from pympler import classtracker, tracker
import logging
import sys
import time
"""
Master entry point
"""

debug = None

def run_irm():
    """
    Starts the intelligent resource management system, which enables autoscaling features
    """
    if Setting.get_autoscaling():    
        IntelligentResourceManager.start_irm(BinPacking.first_fit)
        if IntelligentResourceManager.container_manager:
            SysOut.out_string("IRM Service started!")
        else:
            SysOut.terminate_string("Error: could not start IRM service")



def run_rest_service():
    """
    Run rest as in a thread function
    """
    rest = RESTService()
    rest.run()


def run_msg_service():
    """
    Run msg service to eliminate back pressure
    """
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
    debug = input("Debug mode?  y/n\n")
    if debug == "y":
        SysOut.debug = True
        LookUpTable.debugging = True

    # Load configuration from file
    Setting.read_cfg_from_file()

    # Print instance information
    SysOut.out_string("Node name: " + Setting.get_node_name())
    SysOut.out_string("Node address: " + Setting.get_node_addr())
    SysOut.out_string("Node port: " + str(Setting.get_node_port()))

    # Create thread for handling REST Service
    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor()

    # Start the IRM system
    pool.submit(run_irm)

    # Run messaging system service
    pool.submit(run_msg_service)

    # Binding commander to the rest service and enable REST service
    pool.submit(run_rest_service)

    ### profiling work
    profiler_log_file = "profiling_out.txt"

    tr = tracker.SummaryTracker()
    
    lut_ctr = classtracker.ClassTracker()
    lut_ctr.track_class(LookUpTable)
       
    irm_ctr = classtracker.ClassTracker()
    irm_ctr.track_class(IntelligentResourceManager)
    
    logging.basicConfig(filename=profiler_log_file)
    SysOut.out_string("Logging started!")
    logging.info("Started logging!")
    
    lut_ctr.create_snapshot()
    irm_ctr.create_snapshot()
    try:
        while True:
            time.sleep(60)
            logging.info("\nSummaryTracker:\n{}\n".format(tr.format_diff()))
            from .messaging_system import MessagesQueue
            logging.info("Queue length: {}\n".format(MessagesQueue.verbose()))


    except KeyboardInterrupt:
        lut_ctr.create_snapshot()
        irm_ctr.create_snapshot()
        with open(profiler_log_file, "a") as f:
            sys.stdout = f
            print(" ------------- RESULTS ------------- \n")
            print("LookUpTable:\n")
            lut_ctr.stats.print_summary()
            print("\n\nIRM:\n")
            irm_ctr.stats.print_summary()

        exit

