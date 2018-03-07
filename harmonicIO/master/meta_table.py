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
            ret[Definition.Container.Status.get_str_sid()] = req.params[Definition.Container.Status.get_str_sid()]
            ret[Definition.get_str_last_update()] = Services.get_current_timestamp()
            
            return ret

        @staticmethod
        def verbose():
            return LookUpTable.Containers.__containers

        @staticmethod
        def update_container(dict_input):
            if dict_input[Definition.Container.get_str_con_image_name()] not in LookUpTable.Containers.__containers:
                LookUpTable.Containers.__containers[dict_input[Definition.Container.get_str_con_image_name()]] = []

            if not dict_input in LookUpTable.Containers.__containers[dict_input[Definition.Container.get_str_con_image_name()]]:
                LookUpTable.Containers.__containers[dict_input[Definition.Container.get_str_con_image_name()]].append(dict_input)

        @staticmethod
        def get_candidate_container(image_name):
            if image_name not in LookUpTable.Containers.__containers:
                return None

            if len(LookUpTable.Containers.__containers[image_name]) > 0:
                return LookUpTable.Containers.__containers[image_name].pop()

            return None

        @staticmethod
        def del_container(container_name, short_id):
            conts = LookUpTable.Containers.__containers.get(container_name)
            if not conts:
                return False
            else: 
                # conts is list of containers with same c_name
                
                # List filter code based on: https://stackoverflow.com/questions/1235618/python-remove-dictionary-from-list
                # Removes item with specified short_id from list
                conts[:] = [con for con in conts if con.get(Definition.Container.Status.get_str_sid()) != short_id]
            
            return True




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
            new_item['volatile'] = request.get('volatile')
            LookUpTable.Jobs.__jobs[new_id] = new_item

            return True

        @staticmethod
        def update_job(request):
            job_id = request.get('job_id')
            if not job_id in LookUpTable.Jobs.__jobs:
                SysOut.warn_string("Couldn't update job, no existing job matching ID!")
                return False

            tkn = request.get(Definition.get_str_token())
            if not tkn == LookUpTable.Jobs.__jobs[job_id]['user_token']:
                SysOut.warn_string("Incorrect token, refusing update.")
                return False

            old_job = LookUpTable.Jobs.__jobs[job_id]
            old_job['job_status'] = request.get('job_status')

            return True

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
        return LookUpTable.Jobs.update_job(request)

    @staticmethod
    def poll_id(id):
        return id in LookUpTable.Jobs.verbose()

    @staticmethod
    def remove_container(c_name, csid):
        return LookUpTable.Containers.del_container(c_name, csid)

    @staticmethod
    def verbose():
        ret = dict()
        ret['WORKERS'] = LookUpTable.Workers.verbose()
        ret['CONTAINERS'] = LookUpTable.Containers.verbose()
        ret['TUPLES'] = LookUpTable.Tuples.verbose()
        ret['JOBS'] = LookUpTable.Jobs.verbose()

        return ret
