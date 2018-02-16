import falcon
from .configuration import Setting
from harmonicIO.general.services import SysOut, Services
from .docker_service import DockerService
from harmonicIO.general.definition import Definition, CRole
import json

class ContainerService(object):
    def __init__(self):
        pass

    def on_get(self, req, res):
        """
        GET: docker?token=None&command={command}
        """
        if not Definition.get_str_token() in req.params:
            res.body = "Token is required."
            res.content_type = "String"
            res.status = falcon.HTTP_401
            return

        if not Definition.Docker.get_str_command() in req.params:
            res.body = "Command is required."
            res.content_type = "String"
            res.status = falcon.HTTP_401
            return

        # Check for status command
        if req.params[Definition.Docker.get_str_command()] == Definition.Docker.get_str_status():
            body = DockerService.get_containers_status()
            res.body = str(body)
            res.content_type = "String"
            res.status = falcon.HTTP_200

        ## TODO: add list containers

    def on_post(self, req, res):
        """
        POST: docker?token=None&command={command}
        """
        if not Definition.get_str_token() in req.params:
            res.body = "Token is required."
            res.content_type = "String"
            res.status = falcon.HTTP_401
            return

        if not Definition.Docker.get_str_command() in req.params:
            res.body = "Command is required."
            res.content_type = "String"
            res.status = falcon.HTTP_401
            return

        """
        POST: docker?token=None&command=create
        """
        if req.params[Definition.Docker.get_str_command()] == Definition.Docker.get_str_create():
            # Unpack the posted data
            raw = req.stream.read(req.content_length or 0)
            data = json.loads(str(raw, 'utf-8')) # create dict of body data if it exists

            if not data[Definition.Container.get_str_con_image_name()]:
                res.body = "Required parameters are not supplied!"
                res.content_type = "String"
                res.status = falcon.HTTP_401

            result = DockerService.create_container(data[Definition.Container.get_str_con_image_name()])

            if result:
                res.body = "Okay"
                res.content_type = "String"
                res.status = falcon.HTTP_200
                return
            else:
                res.body = "Create container error!"
                res.content_type = "String"
                res.status = falcon.HTTP_400


class RequestStatus(object):
    def __init__(self):
        # No commander is needed for binding this task
        pass

    def on_get(self, req, res):
        """
        GET: /status?token={None}
        """
        if not Definition.get_str_token() in req.params:
            res.body = "Token is required."
            res.content_type = "String"
            res.status = falcon.HTTP_401
            return

        if req.params[Definition.get_str_token()] == Setting.get_token():
            s_content = Services.get_machine_status(Setting, CRole.WORKER)
            s_content[Definition.REST.get_str_docker()] = DockerService.get_containers_status()
            s_content[Definition.REST.get_str_local_imgs()] = DockerService.get_local_images()

            res.body = str(s_content)

            res.content_type = "String"
            res.status = falcon.HTTP_200
        else:
            res.body = "Invalid token ID."
            res.content_type = "String"
            res.status = falcon.HTTP_401


class RESTService(object):
    def __init__(self):
        # Initialize REST Services
        from wsgiref.simple_server import make_server
        api = falcon.API()

        # Add route for getting status update
        api.add_route('/' + Definition.REST.get_str_status(), RequestStatus())

        # Add route for docker
        api.add_route('/' + Definition.REST.get_str_docker(), ContainerService())

        # Establishing a REST server
        self.__server = make_server(Setting.get_node_internal_addr(), Setting.get_node_port(), api)

    def run(self):
        SysOut.out_string("REST Ready.....")
        self.__server.serve_forever()
