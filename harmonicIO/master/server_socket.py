import socketserver
from .messaging_system import MessagesQueue
from harmonicIO.general.services import SysOut

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    Definition class
    """
    pass


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    """
    This class is the main class that handle with requests from clients.
    the actual mechanism that pass the data to clients.
    """

    def handle(self):
        # Receive and interpret the request data
        data = bytearray()

        """
        Discard heading for now
        data += self.request.recv(16)

        # Interpret the header for file size
        file_size = struct.unpack(">Q", data[8:16])[0]
        """
        try:
            c = True
            while c != b"":
                c = self.request.recv(2048)
                data += c

            # Extract byte header 3 bytes
            image_name_length = int(data[0:3].decode('UTF-8'))
            tcr = image_name_length + 3
            image_name_string = data[3:tcr].decode('UTF-8')

            # Then, push data messaging system.
            MessagesQueue.push_to_queue(image_name_string, data[tcr:])

        except:
            from harmonicIO.general.services import Services
            SysOut.err_string("Insufficient memory for storing g object.")
