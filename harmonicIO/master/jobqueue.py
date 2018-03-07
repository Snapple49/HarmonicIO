import queue
import json
from urllib.request import urlopen
from .meta_table import LookUpTable
from harmonicIO.general.definition import Definition, JobStatus
from harmonicIO.general.services import SysOut
import time
from .messaging_system import MessagesQueue

class JobManager:
    
    def __init__(self, interval, threshold, increment, queuers):
        self.__supervisor_interval = interval
        self.__supervisor_increment = increment
        self.__supervisor_threshold = threshold
        self.queuer_threads = queuers
    

    def find_available_worker(self, container):
        candidates = []
        workers = LookUpTable.Workers.verbose()

        if not workers:
            return None

        # loop through workers and make tuples of worker IP, load and if requested container is available locally
        for worker in workers:
            curr_worker = workers[worker]
            if container in curr_worker[Definition.REST.get_str_local_imgs()]:
                candidates.append((curr_worker[Definition.get_str_node_addr()], curr_worker[Definition.get_str_load5()], True))
            else:
                candidates.append((curr_worker[Definition.get_str_node_addr()], curr_worker[Definition.get_str_load5()], False))

        candidates.sort(key=lambda x: (-x[2], x[1])) # sort candidate workers first on availability of image, then on load (avg load last 5 mins)
        for candidate in candidates:
            if float(candidate[1]) < 0.5: 
                return candidate
        
        return None

    def start_job(self, target_worker, job_data):
        # send request to worker
        worker_url = "http://{}:8081/docker?token=None&command=create".format(target_worker)
        req_data = bytes(json.dumps(job_data), 'utf-8') 
        resp = urlopen(worker_url, req_data) # NOTE: might need increase in timeout to allow download of large container images!!!

        if resp.getcode() == 200: # container was created
            sid = str(resp.read(), 'utf-8')
            SysOut.debug_string("Received sid from container: " + sid)
            return sid
        return False

    def job_queuer(self):
        while True:
            job_data = JobQueue.q.get()
            num_of_conts = job_data.get('num')
            job_sids = []
            for i in range(num_of_conts):
                target = self.find_available_worker(job_data.get(Definition.Container.get_str_con_image_name()))
                try:
                    worker_ip = target[0]
                    sid = self.start_job(worker_ip, job_data)
                    if sid:
                        job_sids.append(sid)
                    if len(job_sids) == num_of_conts:
                        job_data['job_status'] = JobStatus.READY
                        job_data[Definition.Container.Status.get_str_sid()] = job_sids
                except:
                    SysOut.err_string("Response from worker threw exception!")
                    job_data['job_status'] = JobStatus.FAILED
                    break # break makes it stop trying to create new containers as soon as one fails, is this desireable?
            
            ## NOTE: can get really ugly, need to cleanup containers that started (rollback) OR let user know how many were started instead?? or retry failed ones?
            LookUpTable.Jobs.update_job(job_data)
            JobQueue.q.task_done()

    def queue_supervisor(self):
        while True:
            time.sleep(self.__supervisor_interval) ## NOTE: this is probably a very tuneable parameter for later
            msg_queue = MessagesQueue.verbose()
            for container in msg_queue:
                if int(msg_queue[container]) > self.__supervisor_threshold:
                    job_data = {
                        Definition.Container.get_str_con_image_name() : container,
                        'num' : self.__supervisor_increment,
                        'volatile' : True
                    }
                    JobQueue.queue_new_job(job_data)

            

class JobQueue:
    q = queue.Queue()

    @staticmethod
    def queue_new_job(job_data):
        JobQueue.q.put(job_data)