from harmonicIO.general.services import SysOut


class Setting(object):
    __node_name = None
    __node_addr = None
    __node_port = None
    __node_data_port_start = None
    __node_data_port_stop = None
    __std_idle_time = None
    __token = "None"

    @staticmethod
    def set_node_addr(addr=None):
        if addr:
            Setting.__node_addr = addr
        else:
            import socket
            from harmonicIO.general.services import Services
            Setting.__node_addr = socket.gethostname()
            SysOut.debug_string(Setting.__node_addr)
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
    def get_node_addr():
        return Setting.__node_addr

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
    def get_token():
        return Setting.__token

    @staticmethod
    def read_cfg_from_file():
        from harmonicIO.general.services import Services, SysOut
        if not Services.is_file_exist('harmonicIO/master/configuration.json'):
            SysOut.terminate_string('harmonicIO/master/configuration.json does not exist!')
        else:
            with open('harmonicIO/master/configuration.json', 'rt') as t:
                import json
                cfg = json.loads(t.read())

                try:
                    from harmonicIO.general.definition import Definition
                    # Check for the json structure
                    if  Definition.get_str_node_name() in cfg and \
                        Definition.get_str_node_port() in cfg and \
                        Definition.get_str_master_addr() in cfg and \
                        Definition.get_str_data_port_range() in cfg and \
                        Definition.get_str_idle_time() in cfg:
                        # Check port number is int or not
                        if not isinstance(cfg[Definition.get_str_node_port()], int):
                            SysOut.terminate_string("Node port must be integer!")
                        if not isinstance(cfg[Definition.get_str_master_addr()], str):
                            SysOut.terminate_string("Master address must be string!")
                        elif not isinstance(cfg[Definition.get_str_data_port_range()], list):
                            SysOut.terminate_string("Port range must be list!")
                        elif not (isinstance(cfg[Definition.get_str_data_port_range()][0], int) and \
                                  isinstance(cfg[Definition.get_str_data_port_range()][1], int)):
                            SysOut.terminate_string("Port range must be integer!")
                        elif len(cfg[Definition.get_str_data_port_range()]) != 2:
                            SysOut.terminate_string("Port range must compost of two elements: start, stop!")
                        elif not isinstance(cfg[Definition.get_str_idle_time()], int):
                            SysOut.terminate_string("Idle time must be integer!")
                        elif cfg[Definition.get_str_data_port_range()][0] > \
                             cfg[Definition.get_str_data_port_range()][1]:
                            SysOut.terminate_string("Start port range must greater than stop port range!")
                        else:
                            Setting.__node_name = cfg[Definition.get_str_node_name()].strip()
                            Setting.__node_port = cfg[Definition.get_str_node_port()]
                            Setting.__node_data_port_start = cfg[Definition.get_str_data_port_range()][0]
                            Setting.__node_data_port_stop = cfg[Definition.get_str_data_port_range()][1]
                            Setting.__std_idle_time = cfg[Definition.get_str_idle_time()]
                            SysOut.out_string("Load setting successful.")

                        try:
                            if cfg[Definition.get_str_master_addr()].lower() == "auto":
                                Setting.__node_addr = Services.get_host_name_i()
                                SysOut.out_string("Assigning master ip address automatically.")
                            elif Services.is_valid_ipv4(cfg[Definition.get_str_master_addr()]) or \
                                 Services.is_valid_ipv6(cfg[Definition.get_str_master_addr()]):
                                Setting.set_node_addr(cfg[Definition.get_str_master_addr()])
                            else:
                                SysOut.terminate_string("Invalid master IP address format!")

                        except:
                            SysOut.terminate_string("Cannot assign IP address to the master!")

                    else:
                        SysOut.terminate_string("Invalid data in configuration file.")
                except:
                    SysOut.terminate_string("Invalid data in configuration file.")
