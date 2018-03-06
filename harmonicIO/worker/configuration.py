from harmonicIO.general.services import SysOut


class Setting(object):
    __node_name = None
    __node_port = None
    __node_data_port_start = None
    __node_data_port_stop = None
    __std_idle_time = None
    __token = "None"
    __master_addr = None
    __master_port = None
    __node_external_addr = None
    __node_internal_addr = None
    __container_idle_timeout = None

    @staticmethod
    def set_node_addr(addr=None):
        if addr:
            Setting.__node_addr = addr
        else:
            import socket
            from harmonicIO.general.services import Services
            Setting.__node_addr = socket.gethostname()

            # if addr is valid
            if Services.is_valid_ipv4(Setting.__node_addr) or Services.is_valid_ipv6(Setting.__node_addr):
                return None

            # if addr is not valid
            Setting.__node_addr = Services.get_host_name_i()
            if Services.is_valid_ipv4(Setting.__node_addr) or Services.is_valid_ipv6(Setting.__node_addr):
                return None

            SysOut.terminate_string("Cannot get node ip address!")

    @staticmethod
    def get_node_name():
        return Setting.__node_name

    @staticmethod
    def get_node_internal_addr():
        return Setting.__node_internal_addr

    @staticmethod
    def get_node_external_addr():
        return Setting.__node_external_addr

    @staticmethod
    def get_node_addr():
        if Setting.__node_external_addr:
            return Setting.__node_external_addr

        return Setting.__node_internal_addr

    @staticmethod
    def get_node_port():
        return Setting.__node_port

    @staticmethod
    def get_data_port_start():
        return Setting.__node_data_port_start

    @staticmethod
    def get_data_port_stop():
        return Setting.__node_data_port_stop

    @staticmethod
    def get_std_idle_time():
        return Setting.__std_idle_time

    @staticmethod
    def get_master_addr():
        return Setting.__master_addr

    @staticmethod
    def get_master_port():
        return Setting.__master_port

    @staticmethod
    def get_token():
        return Setting.__token

    @staticmethod
    def get_min_worker():
        return 1

    @staticmethod
    def get_node_external_addr():
        return Setting.__node_external_addr
        
    @staticmethod
    def get_container_idle_timeout():
        return Setting.__container_idle_timeout

    @staticmethod
    def read_cfg_from_file():
        from harmonicIO.general.services import Services
        if not Services.is_file_exist('harmonicIO/worker/configuration.json'):
            SysOut.terminate_string('harmonicIO/worker/configuration.json does not exist')
        else:
            with open('harmonicIO/worker/configuration.json', 'rt') as t:
                import json
                cfg = json.loads(t.read())

                try:
                    from harmonicIO.general.definition import Definition
                    # Check for the json structure
                    if  Definition.get_str_node_name() in cfg and \
                        Definition.get_str_node_port() in cfg and \
                        Definition.get_str_data_port_range() in cfg and \
                        Definition.get_str_idle_time() in cfg and \
                        Definition.get_str_master_addr() in cfg and \
                        Definition.get_str_master_port() in cfg and \
                        Definition.get_str_container_idle_timeout() in cfg and \
                        Definition.get_str_node_internal_addr() in cfg and \
                        Definition.get_str_node_internal_addr():
                        # Check port number is int or not
                        if not isinstance(cfg[Definition.get_str_node_port()], int):
                            SysOut.terminate_string("Node port must be integer.")
                        elif not isinstance(cfg[Definition.get_str_data_port_range()], list):
                            SysOut.terminate_string("Port range must be list.")
                        elif not (isinstance(cfg[Definition.get_str_data_port_range()][0], int) and \
                                  isinstance(cfg[Definition.get_str_data_port_range()][1], int)):
                            SysOut.terminate_string("Port range must be integer.")
                        elif not isinstance(cfg[Definition.get_str_master_port()], int):
                            SysOut.terminate_string("Master port must be integer.")
                        elif len(cfg[Definition.get_str_data_port_range()]) != 2:
                            SysOut.terminate_string("Port range must compost of two elements: start, stop.")
                        elif not isinstance(cfg[Definition.get_str_idle_time()], int):
                            SysOut.terminate_string("Idle time must be integer.")
                        elif cfg[Definition.get_str_data_port_range()][0] > \
                             cfg[Definition.get_str_data_port_range()][1]:
                            SysOut.terminate_string("Start port range must greater than stop port range.")
                        else:
                            Setting.set_node_addr()
                            import multiprocessing
                            Setting.__node_name = cfg[Definition.get_str_node_name()].strip()
                            Setting.__node_port = cfg[Definition.get_str_node_port()]
                            Setting.__node_data_port_start = cfg[Definition.get_str_data_port_range()][0]
                            Setting.__node_data_port_stop = cfg[Definition.get_str_data_port_range()][1]
                            Setting.__std_idle_time = cfg[Definition.get_str_idle_time()]
                            Setting.__master_addr = cfg[Definition.get_str_master_addr()].strip()
                            Setting.__master_port = cfg[Definition.get_str_master_port()]
                            Setting.__node_external_addr = cfg[Definition.get_str_node_external_addr()].strip().lower()
                            Setting.__container_idle_timeout = cfg[Definition.get_str_container_idle_timeout()] * 60 # convert idle time from minute to seconds

                            # Check for auto node name
                            if Setting.__node_name.lower() == "auto":
                                # Get node name from host name
                                import socket
                                Setting.__node_name = socket.gethostname()

                            # Check for overriding node address
                            if cfg[Definition.get_str_node_internal_addr()] and \
                               cfg[Definition.get_str_node_internal_addr()] != "auto":
                                # Set node name automatically from hostname
                                from harmonicIO.general.services import Services

                                if Services.is_valid_ipv4(cfg[Definition.get_str_node_internal_addr()]) or \
                                   Services.is_valid_ipv6(cfg[Definition.get_str_node_internal_addr()]):
                                    Setting.__node_internal_addr = cfg[Definition.get_str_node_internal_addr()]

                            # Check for node address validity
                            if Setting.get_node_external_addr() != "none":
                                from harmonicIO.general.services import Services

                                if Services.is_valid_ipv4(Setting.get_node_external_addr()) or \
                                   Services.is_valid_ipv6(Setting.get_node_external_addr()):
                                    SysOut.out_string("By pass request with external address.")
                                else:
                                    SysOut.terminate_string("Invaliid external ip address!")
                            else:
                                Setting.__node_external_addr = None

                            SysOut.out_string("Load setting successful.")
                    else:
                        SysOut.terminate_string("Required parameters are not present.")
                except Exception as e:
                    print(e)
                    SysOut.terminate_string("Invalid data in configuration file.")
