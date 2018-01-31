"""
This module contain information about the master node and its connector
"""
import urllib3
import time
import socket
import hashlib
from harmonicIO.general.services import SysOut, Services
from harmonicIO.general.definition import Definition, CRole


class LocalError(object):

    @staticmethod
    def err_invalid_port():
        SysOut.terminate_string("Invalid port data type! Require int, but got others!")

    @staticmethod
    def err_invalid_ip():
        SysOut.terminate_string("Invalid Server IP address!")

    @staticmethod
    def err_invalid_token_type():
        SysOut.terminate_string("Invalid token data type! Require string, but got others!")

    @staticmethod
    def err_invalid_max_try_type():
        SysOut.terminate_string("Invalid max_try type! Require int, but got others!")

    @staticmethod
    def err_invalid_priority_type():
        SysOut.terminate_string("Invalid priority type! Require int, but got others!")

    @staticmethod
    def err_invalid_data_container_type():
        SysOut.terminate_string("Invalid data type! Require ByteArray, but got others")


class StreamConnector(object):

    def __init__(self, server_addr, server_port, token="None", std_idle_time=0, max_try=9, source_name=None):
        # Check instance type
        if not isinstance(server_port, int):
            LocalError.err_invalid_port()

        # Check for server address
        if not (Services.is_valid_ipv4(server_addr) or Services.is_valid_ipv6(server_addr)):
            LocalError.err_invalid_ip()

        # Check token type
        if not isinstance(token, str):
            LocalError.err_invalid_token_type()

        # Check max try type
        if not isinstance(max_try, int):
            LocalError.err_invalid_max_try_type()

        """
        System definitions.
        """
        self.__master_addr = server_addr
        self.__master_port = server_port
        self.__master_token = token
        self.__std_idle_time = std_idle_time
        self.__max_try = max_try

        if source_name:
            self.__source_name = source_name
        else:
            import socket
            self.__source_name = socket.gethostname()

        # Connection string
        from harmonicIO.general.definition import Definition

        self.__str_master_status = Definition.Master.get_str_check_master(server_addr, server_port, "None")
        self.__str_push_request = Definition.Master.get_str_push_req(server_addr, server_port, "None")

        # URL Request
        self.__connector = urllib3.PoolManager()

    def is_master_alive(self):
        """
        Check for the master status that is it alive or not!
        :return: Boolean
        """
        try:
            response = self.__connector.request('GET', self.__str_master_status)

            if response.status == 200:
                return True
        except:
            return False

    def __get_stream_end_point(self, container_name, container_os, priority, digest):
        """
        Request for the stream end point from the master.
        :return: Boolean(False) when the system is busy.
                 Tuple(batch_addr, batch_port, tuple_id) if the batch or messaging system is available.
        """

        if not priority:
            priority = 0
        else:
            if not isinstance(priority, int):
                LocalError.err_invalid_priority_type()

        try:

            url = self.__str_push_request + Definition.Master.get_str_push_req_container_ext(container_name,
                                                                                             container_os, priority,
                                                                                             self.__source_name,
                                                                                             digest)

            print('Sending request..')
            print(url)

            response = self.__connector.request('GET',
                                                url)
            # print(response.status)
            # print(response.text)

            if response.status == 406:
                # Messages in queue is full. Result in queue lock.
                SysOut.warn_string("Queue in master is full.")
                return False

            if response.status == 500:
                SysOut.warn_string("System internal error! Please consult documentation.")
                return False

            elif response.status != 200:
                SysOut.warn_string("something else went wrong")
                return False

        except Exception as ex:
            print(ex)
            SysOut.err_string("Couldn't connect to the master at {0}:{1}.".format(self.__master_addr,
                                                                                  self.__master_port))
            return False

        try:
            content = eval(response.data.decode('utf-8'))
            return content

        except:
            SysOut.warn_string("JSON content error from the master!")
            return False

    def __push_stream_end_point(self, t_addr, t_port, data):
        """
        Create a client socket to connect to server
        :param target: Tuple with three parameter from the endpoint request
        :param data: ByteArray which holds the content to be streamed to the batch.
        :return: Boolean return status
        """

        try:
            s = None
            for res in socket.getaddrinfo(t_addr, t_port, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, socktype, proto, canonname, sa = res
                try:
                    s = socket.socket(af, socktype, proto)
                except OSError as msg:
                    print(msg)
                    s = None
                    continue
                try:
                    s.connect(sa)
                except OSError as msg:
                    print(msg)
                    s.close()
                    s = None
                    continue
                break
            if s is None:
                SysOut.warn_string("Cannot connect to " + t_addr + ":" + str(t_port) + "!")
                return False

            with s:
                # Identifying object id
                s.sendall(data)
                s.sendall(b'')
                s.close()

            return True

        except:
            SysOut.warn_string("Cannot stream data to an end point!")

    def __push_stream_end_point_MS(self, t_addr, t_port, data, image_name):
        """
        Create a client socket to connect to server
        :param target: Tuple with three parameter from the endpoint request
        :param data: ByteArray which holds the content to be streamed to the batch.
        :return: Boolean return status
        """

        try:
            s = None
            for res in socket.getaddrinfo(t_addr, t_port, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, socktype, proto, canonname, sa = res
                try:
                    s = socket.socket(af, socktype, proto)
                except OSError as msg:
                    print(msg)
                    s = None
                    continue
                try:
                    s.connect(sa)
                except OSError as msg:
                    print(msg)
                    s.close()
                    s = None
                    continue
                break
            if s is None:
                SysOut.warn_string("Cannot connect to " + t_addr + ":" + str(t_port) + "!")
                return False

            # Generate header string
            image_name_b = bytes(image_name, 'UTF-8')
            image_name_l = str(len(image_name_b))

            while len(image_name_l) < 3:
                image_name_l = "0" + image_name_l

            image_name_t = bytes(image_name_l, 'UTF-8') + image_name_b

            with s:
                # Identifying object id
                s.sendall(image_name_t)
                s.sendall(data)
                s.sendall(b'')
                s.close()

            return True

        except:
            SysOut.warn_string("Cannot stream data to an end point!")

    def send_data(self, container_name, container_os, data, priority=None):
        # The data must be byte array
        if not isinstance(data, bytearray):
            LocalError.err_invalid_data_container_type()

        if len(data) == 0:
            SysOut.err_string("No content in byte array.")
            return None

        digest = hashlib.md5(data).hexdigest()

        end_point = None

        counter = self.__max_try
        while not end_point:
            end_point = self.__get_stream_end_point(container_name, container_os, priority, digest)
            counter -= 1
            if counter == 0:
                SysOut.err_string("Cannot contact server. Exceed maximum retry {0}!".format(self.__max_try))
                return False

        # Send data to worker for processing directly
        counter = self.__max_try
        if end_point[Definition.get_str_node_role()] == CRole.WORKER:
            while not self.__push_stream_end_point(end_point[Definition.get_str_node_addr()],
                                                   end_point[Definition.get_str_node_port()],
                                                   data):
                time.sleep(self.__std_idle_time)
                counter -= 1
                if counter == 0:
                    SysOut.err_string("Cannot contact server. Exceed maximum retry {0}!".format(self.__max_try))
                    return False

        # Send data to master for queuing (?)
        elif end_point[Definition.get_str_node_role()] == CRole.MESSAGING_SYSTEM:
            while not self.__push_stream_end_point_MS(end_point[Definition.get_str_node_addr()],
                                                      end_point[Definition.get_str_node_port()],
                                                      data,
                                                      container_name):
                time.sleep(self.__std_idle_time)
                counter -= 1
                if counter == 0:
                    SysOut.err_string("Cannot contact server. Exceed maximum retry {0}!".format(self.__max_try))
                    return False
        else:
            return False

        if end_point[Definition.get_str_node_role()] == CRole.WORKER:
            SysOut.out_string(
                "Push data to worker ({0}:{1}>{2}) successful.".format(end_point[Definition.get_str_node_addr()],
                                                                       end_point[Definition.get_str_node_port()],
                                                                       container_name))
        elif end_point[Definition.get_str_node_role()] == CRole.MESSAGING_SYSTEM:
            SysOut.out_string("Push data to messaging system ({0}:{1}>{2}) successful.".format(
                end_point[Definition.get_str_node_addr()],
                end_point[Definition.get_str_node_port()],
                container_name))
        else:
            SysOut.out_string(
                "Push data to unknown ({0}:{1}>{2}) successful.".format(end_point[Definition.get_str_node_addr()],
                                                                        end_point[Definition.get_str_node_port()],
                                                                        container_name))

    def get_data_container(self):
        # Can be override to byte array with pre-defined header.
        return bytearray()
