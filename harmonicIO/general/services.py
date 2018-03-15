import os.path
import subprocess
from sys import platform
from .colors import red, green, yellow, blue
from .definition import Definition, CRole


class SysOut(object):

    @staticmethod
    def warn_string(msg):
        print(yellow("[WARN: " + msg + "]"))

    @staticmethod
    def out_string(msg):
        print(green("[OUT: " + msg + "]"))

    @staticmethod
    def err_string(msg):
        print(red("[ERR: " + msg + "]"))

    @staticmethod
    def terminate_string(msg, terminate_code=-1):
        print(red("[ERR-EXIT: " + msg + "]"))
        exit(terminate_code)

    @staticmethod
    def debug_string(msg):
        print(blue("[DEB: " + msg + "]"))

    @staticmethod
    def usr_string(msg):
        print(msg)


class Services(object):
    """
    Service method, check for file exist in the local machine
    """
    @staticmethod
    def is_file_exist(file):
        return os.path.exists(file)

    @staticmethod
    def is_str_is_digit(msg):
        return msg.isdigit()

    @staticmethod
    def is_folder_exist(folder):
        return os.path.isdir(folder)

    @staticmethod
    def get_host_name_i(order=0):
        # get hostname as IP address (-i)
        if platform == "linux" or platform == "linux2":
            import subprocess
            # get hostname as IP address (-i)
            return subprocess.check_output(["hostname", "-I"]).decode('utf-8').strip().split()[order]
        elif platform == "darwin":  # OS X
            # en0 = first, en1 = second.
            import subprocess
            return subprocess.check_output(["ipconfig", "getifaddr", "en0"]).decode('utf-8').strip()
        elif platform == "win32":
            # Windows...
            raise Exception('not implemented!')

    @staticmethod
    def get_current_timestamp():
        import time
        return int(time.time())

    @staticmethod
    def get_machine_status(setting, role):

        # Get load value
        res = str(subprocess.check_output(Definition.get_cpu_load_command())).strip()
        res = res.replace(",", "").replace("\\n", "").replace("'", "")
        *_, load1, load5, load15 = res.split(" ")

        body = dict()
        body[Definition.get_str_node_name()] = setting.get_node_name()
        body[Definition.get_str_node_role()] = role
        body[Definition.get_str_node_addr()] = setting.get_node_addr()
        body[Definition.get_str_node_port()] = setting.get_node_port()
        body[Definition.get_str_load1()] = load1
        body[Definition.get_str_load5()] = load5
        body[Definition.get_str_load15()] = load15

        return body

    @staticmethod
    def is_valid_ipv4(ip):
        import re
        pattern = re.compile(r"""
            ^
            (?:
              # Dotted variants:
              (?:
                # Decimal 1-255 (no leading 0's)
                [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
              |
                0x0*[0-9a-f]{1,2}  # Hexadecimal 0x0 - 0xFF (possible leading 0's)
              |
                0+[1-3]?[0-7]{0,2} # Octal 0 - 0377 (possible leading 0's)
              )
              (?:                  # Repeat 0-3 times, separated by a dot
                \.
                (?:
                  [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
                |
                  0x0*[0-9a-f]{1,2}
                |
                  0+[1-3]?[0-7]{0,2}
                )
              ){0,3}
            |
              0x0*[0-9a-f]{1,8}    # Hexadecimal notation, 0x0 - 0xffffffff
            |
              0+[0-3]?[0-7]{0,10}  # Octal notation, 0 - 037777777777
            |
              # Decimal notation, 1-4294967295:
              429496729[0-5]|42949672[0-8]\d|4294967[01]\d\d|429496[0-6]\d{3}|
              42949[0-5]\d{4}|4294[0-8]\d{5}|429[0-3]\d{6}|42[0-8]\d{7}|
              4[01]\d{8}|[1-3]\d{0,9}|[4-9]\d{0,8}
            )
            $
        """, re.VERBOSE | re.IGNORECASE)
        return pattern.match(ip) is not None

    @staticmethod
    def is_valid_ipv6(ip):
        import re
        pattern = re.compile(r"""
            ^
            \s*                         # Leading whitespace
            (?!.*::.*::)                # Only a single whildcard allowed
            (?:(?!:)|:(?=:))            # Colon iff it would be part of a wildcard
            (?:                         # Repeat 6 times:
                [0-9a-f]{0,4}           #   A group of at most four hexadecimal digits
                (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
            ){6}                        #
            (?:                         # Either
                [0-9a-f]{0,4}           #   Another group
                (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
                [0-9a-f]{0,4}           #   Last group
                (?: (?<=::)             #   Colon iff preceeded by exacly one colon
                 |  (?<!:)              #
                 |  (?<=:) (?<!::) :    #
                 )                      # OR
             |                          #   A v4 address with NO leading zeros
                (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
                (?: \.
                    (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
                ){3}
            )
            \s*                         # Trailing whitespace
            $
        """, re.VERBOSE | re.IGNORECASE | re.DOTALL)
        return pattern.match(ip) is not None
