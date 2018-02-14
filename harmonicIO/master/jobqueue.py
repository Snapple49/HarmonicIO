import queue
import json
from urllib.request import urlopen
from .meta_table import LookUpTable
from harmonicIO.general.definition import Definition, JobStatus


class JobManager():

    def find_available_worker(self, container):
        ## TODO: actually do stuff
        return [('192.168.1.9', '0.05')]

    def start_job(self, target_worker, job_data):
        # send request to worker
        worker_url = "http://{}:8081/docker?token=None&command=create".format(target_worker)
        req_data = bytes(json.dumps(job_data), 'utf-8') 
        resp = urlopen(worker_url, req_data)

        if resp.getcode() == 200: # container was created
            return True
        ## TODO: add port?
        return False

    def job_queuer(self):
        while True:
            job_data = JobQueue.q.get()
            print("Got some work!")
            candidates = self.find_available_worker(job_data.get('c_name'))
            worker_ip = candidates[0][0]
            if self.start_job(worker_ip, job_data):
                job_data[Definition.get_str_node_port()] = 1337
                job_data[Definition.get_str_node_addr()] = worker_ip
                job_data['job_status'] = JobStatus.READY
                LookUpTable.Jobs.update_job(job_data) ## TODO: double check nothing wrong gets updated
            print("Completed job!")
            JobQueue.q.task_done()

class JobQueue(object):
    q = queue.Queue()

    @staticmethod
    def queue_new_job(job_data):
        JobQueue.q.put(job_data)