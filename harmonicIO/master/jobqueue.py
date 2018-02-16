import queue
import json
from urllib.request import urlopen
from .meta_table import LookUpTable
from harmonicIO.general.definition import Definition, JobStatus
from harmonicIO.general.services import SysOut

def asdasd(job_req):
    # get server data
    data = LookUpTable.verbose()
    data['MSG'] = MessagesQueue.verbose()
    candidates = []
    target_container = job_req[Definition.Container.get_str_con_image_name()]

    for worker in data["WORKERS"]:
        if worker[Definition.REST.get_str_local_imgs()]:

            for image in worker[Definition.REST.get_str_local_imgs()]:
                if target_container in image.tags:
                    candidate = (worker["node_addr"], worker["load5"]) # create tuple with IP and load on worker with container

    # find suitable worker by prio 1
    if target_container in data["CONTAINERS"]:
        print("Looking for container called " + target_container)
        for container in data["CONTAINERS"][target_container]:
            candidate = ((container["batch_addr"], data["WORKERS"][container["batch_addr"]]["load5"])) # create tuple with IP and load on worker with container
            if candidate[1] < 0.5: # only add candidate if worker load less than 50%
                candidates.append(candidate)


    # find suitable worker by prio 2
    elif data["WORKERS"]:
        for worker in data["WORKERS"]:

            if candidate[1] < 0.5:
                candidates.append(candidate)

    # no suitable worker available
    else:
        return None

    candidates.sort(key=lambda index: index[1]) # sort candidate workers on load (avg. load last 5 minutes)
    print('Candidates:\n' + candidates)
    print(str(candidates[0]) + " has least load, sending request here!")

    return candidates

class JobManager():

    def find_available_worker(self, container):
        ## TODO: actually do stuff
        return [('192.168.1.8', '0.05')]

    def start_job(self, target_worker, job_data):
        # send request to worker
        worker_url = "http://{}:8081/docker?token=None&command=create".format(target_worker)
        req_data = bytes(json.dumps(job_data), 'utf-8') 
        resp = urlopen(worker_url, req_data) # NOTE: might need increase in timeout to allow download of large container images!!!

        if resp.getcode() == 200: # container was created
            return True
        ## TODO: add port?
        return False

    def job_queuer(self):
        while True:
            job_data = JobQueue.q.get()
            candidates = self.find_available_worker(job_data.get('c_name'))
            worker_ip = candidates[0][0]
            try:
                if self.start_job(worker_ip, job_data):
                    job_data[Definition.get_str_node_port()] = 1337
                    job_data[Definition.get_str_node_addr()] = worker_ip
                    job_data['job_status'] = JobStatus.READY
            except:
                job_data['job_status'] = JobStatus.FAILED
            finally:
                LookUpTable.Jobs.update_job(job_data) ## TODO: double check nothing wrong gets updated
                JobQueue.q.task_done()

class JobQueue(object):
    q = queue.Queue()

    @staticmethod
    def queue_new_job(job_data):
        JobQueue.q.put(job_data)