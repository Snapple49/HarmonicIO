import socket
import docker
from .configuration import Setting
from harmonicIO.general.definition import CStatus, Definition
from harmonicIO.general.services import SysOut


class ChannelStatus(object):
    def __init__(self, port):
        self.port = port
        if self.is_port_open():
            self.status = CStatus.BUSY
        else:
            self.status = CStatus.AVAILABLE

    def is_port_open(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', self.port))
        sock.close()

        if result == 0:
            return True

        return False


class DockerMaster(object):

    def __init__(self):
        self.__ports = []

        self.__client = docker.from_env()

        SysOut.out_string("Docker master initialization complete.")

        # Define port status
        for port_num in range(Setting.get_data_port_start(), Setting.get_data_port_stop()):
            self.__ports += [ChannelStatus(port_num)]

        # Check number of available port
        available_port = 0
        for item in self.__ports:
            if item.status == CStatus.AVAILABLE:
                available_port += 1

        self.__available_port = available_port

    def __get_available_port(self):
        for item in self.__ports:
            if item.status == CStatus.AVAILABLE:
                item.status = CStatus.BUSY
                return item.port

        return None

    def get_containers_status(self):

        def get_container_status(input):
            res = dict()
            res[Definition.Container.Status.get_str_sid()] = input.short_id
            res[Definition.Container.Status.get_str_image()] = input.image.tags
            res[Definition.Container.Status.get_str_status()] = input.status
            return res

        res = []
        for item in self.__client.containers.list():
            res.append(get_container_status(item))

        return res

    def run_container(self, container_name):

        def get_ports_setting(expose, ports):
            return {str(expose) + '/tcp': ports}

        def get_env_setting(expose, a_port):
            ret = dict()
            ret[Definition.Docker.HDE.get_str_node_name()] = container_name
            ret[Definition.Docker.HDE.get_str_node_addr()] = Setting.get_node_addr()
            ret[Definition.Docker.HDE.get_str_node_data_port()] = expose
            ret[Definition.Docker.HDE.get_str_node_forward_port()] = a_port
            ret[Definition.Docker.HDE.get_str_master_addr()] = Setting.get_master_addr()
            ret[Definition.Docker.HDE.get_str_master_port()] = Setting.get_master_port()
            ret[Definition.Docker.HDE.get_str_std_idle_time()] = Setting.get_std_idle_time()
            ret[Definition.Docker.HDE.get_str_token()] = Setting.get_token()

            return ret

        port = self.__get_available_port()
        expose_port = 80

        if not port:
            SysOut.err_string("No more port available!")
            return False
        else:

            res = self.__client.containers.run(container_name,
                                               detach=True,
                                               stderr=True,
                                               stdout=True,
                                               ports=get_ports_setting(expose_port, port),
                                               environment=get_env_setting(expose_port, port))
            import time
            time.sleep(10)
            print(res.logs())
            print('hi!')

            if res:
                SysOut.out_string("Container " + container_name + " is created!")
                SysOut.out_string("Container " + container_name + " is " + res.status + " ")
                res.wait()
                return True
            else:
                SysOut.out_string("Container " + container_name + " cannot be created!")
                return False
