import queue
from harmonicIO.general.services import Services, SysOut
from harmonicIO.general.definition import Definition, CTuple


class DataStatStatus(object):
    PENDING = 0
    PROCESSING = 1
    RESTREAM = 2


class LookUpTable(object):

    class Workers(object):
        __workers = {}

        @staticmethod
        def verbose():
            return LookUpTable.Workers.__workers

        @staticmethod
        def add_worker(dict_input):
            dict_input[Definition.get_str_last_update()] = Services.get_current_timestamp()
            LookUpTable.Workers.__workers[dict_input[Definition.get_str_node_addr()]] = dict_input

        @staticmethod
        def del_worker(worker_addr):
            # TODO: implement actual worker termination?
            del LookUpTable.Workers.__workers[worker_addr]

    class Containers(object):
        __containers = {}

        @staticmethod
        def get_container_object(req):
            ret = dict()
            ret[Definition.REST.Batch.get_str_batch_addr()] = req.params[Definition.REST.Batch.get_str_batch_addr()].strip()
            ret[Definition.REST.Batch.get_str_batch_port()] = int(req.params[Definition.REST.Batch.get_str_batch_port()])
            ret[Definition.REST.Batch.get_str_batch_status()] = int(req.params[Definition.REST.Batch.get_str_batch_status()])
            ret[Definition.Container.get_str_con_image_name()] = req.params[Definition.Container.get_str_con_image_name()].strip()
            ret[Definition.get_str_last_update()] = Services.get_current_timestamp()

            return ret

        @staticmethod
        def verbose():
            ret = dict()
            for key, value in LookUpTable.Containers.__containers.items():
                ret[key] = list(value.queue)

            return ret

        @staticmethod
        def update_container(dict_input):
            if dict_input[Definition.Container.get_str_con_image_name()] not in LookUpTable.Containers.__containers:
                LookUpTable.Containers.__containers[dict_input[Definition.Container.get_str_con_image_name()]] = queue.Queue()

            LookUpTable.Containers.__containers[dict_input[Definition.Container.get_str_con_image_name()]].put(dict_input)

        @staticmethod
        def get_candidate_container(image_name):
            if image_name not in LookUpTable.Containers.__containers:
                return None

            if len(LookUpTable.Containers.__containers[image_name].queue) > 0:
                return LookUpTable.Containers.__containers[image_name].get()

            return None

    class Tuples(object):
        __tuples = {}

        @staticmethod
        def get_tuple_object(req):
            # parameters
            ret = dict()
            ret[Definition.Container.get_str_data_digest()] = req.params[Definition.Container.get_str_data_digest()].strip()
            ret[Definition.Container.get_str_con_image_name()] = req.params[Definition.Container.get_str_con_image_name()].strip()
            ret[Definition.Container.get_str_container_os()] = req.params[Definition.Container.get_str_container_os()].strip()
            ret[Definition.Container.get_str_data_source()] = req.params[Definition.Container.get_str_data_source()].strip()
            ret[Definition.Container.get_str_container_priority()] = 0
            ret[Definition.REST.get_str_status()] = CTuple.SC
            ret[Definition.get_str_last_update()] = Services.get_current_timestamp()
            return ret

        @staticmethod
        def get_tuple_id(tuple_info):
            return tuple_info[Definition.Container.get_str_data_digest()][0:12] + ":" + str(tuple_info[Definition.get_str_last_update()])

        @staticmethod
        def add_tuple_info(tuple_info):
            LookUpTable.Tuples.__tuples[LookUpTable.Tuples.get_tuple_id(tuple_info)] = tuple_info

        @staticmethod
        def verbose():
            return LookUpTable.Tuples.__tuples

    class Jobs(object):
        __jobs = {}

        # create new job from request dictionary
        @staticmethod
        def new_job(request):
            new_item = {}
            new_id = request.get('job_id')
            if not new_id:
                SysOut.warn_string("Couldn't create job, no ID provided!")
                return False

            if new_id in LookUpTable.Jobs.__jobs:
                SysOut.warn_string("Job already exists in system, can't create!")
                return False

            new_item['job_id'] = new_id
            new_item['job_status'] = request.get('job_status')
            new_item[Definition.Container.get_str_con_image_name()] = request.get(Definition.Container.get_str_con_image_name())
            new_item['user_token'] = request.get(Definition.get_str_token())
            new_item['time_to_live'] = request.get('ttl')
            new_item['start_time'] = request.get('start_time')
            LookUpTable.Jobs.__jobs[new_id] = new_item

            return True

        @staticmethod
        def update_job(request):
            job_id = request.get('job_id')
            if not job_id in LookUpTable.Jobs.__jobs:
                SysOut.warn_string("Couldn't update job, no existing job matching ID!")
                return None

            tkn = request.get(Definitions.get_str_token())
            if not tkn == LookUpTable.Jobs.__jobs[job_id]['user_token']:
                SysOut.warn_string("Incorrect token, refusing update.")
                return None

            old_job = LookUpTable.Jobs.__jobs[]
            old_job['job_status'] = request.get('job_status')
            old_job[Definition.Container.get_str_con_image_name()] = request.get(Definition.Container.get_str_con_image_name())
            #old_job['user_token'] = request.get(Definition.get_str_token()) # should not be able to change user who requested job?
            if 'ttl' in request:
                old_job['time_to_live'] = request.get('ttl')
            host = {}
            host[Definition.get_str_node_port()] = request.get(Definition.get_str_node_port())
            host[Definition.get_str_node_addr()] = request.get(Definition.get_str_node_addr())
            old_job['host_container'] = host

        @staticmethod
        def verbose():
            return LookUpTable.Jobs.__jobs

    @staticmethod
    def update_worker(dict_input):
        LookUpTable.Workers.add_worker(dict_input)

    @staticmethod
    def get_candidate_container(image_name):
        return LookUpTable.Containers.get_candidate_container(image_name)

    @staticmethod
    def new_job(request):
        return LookUpTable.Jobs.new_job(request)

    @staticmethod
    def update_job(request):
        LookUpTable.Jobs.update_job(request)

    @staticmethod
    def verbose():
        ret = dict()
        ret['WORKERS'] = LookUpTable.Workers.verbose()
        ret['CONTAINERS'] = LookUpTable.Containers.verbose()
        ret['TUPLES'] = LookUpTable.Tuples.verbose()
        ret['JOBS'] = LookUpTable.Jobs.verbose()

        return ret
