from .docker_master import DockerMaster


class DockerService(object):
    __docker_master = None

    @staticmethod
    def init():
        DockerService.__docker_master = DockerMaster()

    @staticmethod
    def create_container(container_name, volatile):
        return DockerService.__docker_master.run_container(container_name, volatile)

    @staticmethod
    def get_containers_status():
        return DockerService.__docker_master.get_containers_status()

    @staticmethod
    def get_local_images():
        return DockerService.__docker_master.get_local_images()

    @staticmethod
    def delete_container(csid):
        return DockerService.__docker_master.delete_container(csid)
