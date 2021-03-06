from __future__ import print_function, division

import yaml
import os
import time
import socket
from hashlib import sha224
from collections import OrderedDict
import glob
import logging

logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler=logging.StreamHandler()
logger.addHandler(handler)
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
handler.setFormatter(formatter)

def log(*args,**kwargs):
    severity=kwargs.get('severity','info').upper()
    logger.log(getattr(logging,severity)," ".join([repr(arg) for arg in list(args)+list(kwargs.items())]))


class Empty(Exception):
    pass

class CurrentTaskUnfinished(Exception):
    pass

class TaskStolen(Exception):
    pass

class Task(object):
    def __init__(self,task_data,execution_info=None, submission_data=None, depends_on=None):
        self.task_data=task_data
        self.submission_info=self.construct_submission_info()
        self.depends_on=depends_on

        if submission_data is not None:
            self.submission_info.update(submission_data)

        self.execution_info=execution_info

    def construct_submission_info(self):
        return dict(
            time=time.time(),
            utc=time.strftime("%Y%m%d-%H%M%S"),
            hostname=socket.gethostname(),
            fqdn=socket.getfqdn(),
            pid=os.getpid(),
        )

    def serialize(self):
        return yaml.dump(dict(
                submission_info=self.submission_info,
                task_data=self.task_data,
                execution_info=self.execution_info,
                depends_on=self.depends_on,
            ),
            default_flow_style=False, default_style=''
        )


    @classmethod
    def from_file(cls,fn):
        try:
            task_dict=yaml.load(open(fn))
        except IOError as e:
            raise TaskStolen()

        if task_dict is None:
            raise TaskStolen("empty: probably not written")

        self=cls(task_dict['task_data'])
        self.depends_on=task_dict['depends_on']
        self.submission_info=task_dict['submission_info']

        return self

    def to_file(self,fn):
        open(fn, "w").write(self.serialize())

    @property
    def filename_instance(self):
        return self.get_filename(False)

    @property
    def filename_key(self):
        return self.get_filename(True)

    def get_filename(self,key=True):
        filename_components=[]

        task_data_string=yaml.dump(self.task_data,encoding='utf-8')

        filename_components.append(sha224(task_data_string).hexdigest()[:8])
        log("encoding: "+repr(filename_components),severity="debug")
        log(task_data_string,severity="debug")

        if not key:
            filename_components.append("%.14lg"%self.submission_info['time'])
            filename_components.append(self.submission_info['utc'])

            filename_components.append(sha224(str(OrderedDict(sorted(self.submission_info.items()))).encode('utf-8')).hexdigest()[:8])

        return "_".join(filename_components)

    def __repr__(self):
        return "[{}: {}]".format(self.__class__.__name__,self.task_data)

def makedir_if_neccessary(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != 17: raise


class Queue(object):
    use_timestamps=False


    def __init__(self,root_directory):
        self.root_directory=root_directory
        self.init_directory_tree()
        self.current_task=None
        self.current_task_status=None

    @property
    def taskname(self):
        return self.current_task.filename_instance

    @property
    def task_fn(self):
        return self.queue_dir(self.current_task_status)+"/"+self.current_task.filename_instance

    def lock(self):
        pass

    def unlock(self):
        pass

    def queue_dir(self,kind):
        return self.root_directory + "/" + kind


    def init_directory_tree(self):
        makedir_if_neccessary(self.queue_dir("waiting"))
        makedir_if_neccessary(self.queue_dir("deleted"))
        makedir_if_neccessary(self.queue_dir("running"))
        makedir_if_neccessary(self.queue_dir("done"))
        makedir_if_neccessary(self.queue_dir("failed"))
        makedir_if_neccessary(self.queue_dir("locked"))

    def find_task_instances(self,task,klist=None):
        if klist is None:
            klist=["waiting", "running", "done", "failed", "locked"]

        instances_for_key = []
        for state in klist:
            instances_for_key+=[
                    dict(state=state,fn=fn) for fn in glob.glob(self.queue_dir(state)+"/"+task.filename_key+"*")
                ]
        return instances_for_key
    
    def try_all_locked(self):
        r=[]
        for task_fn in self.list("locked"):
            log("trying to unlock", task_fn)
            r.append(self.try_to_unlock(Task.from_file(self.queue_dir("locked")+"/"+task_fn)))
        return r

    def try_to_unlock(self,task):
        dependency_states=self.find_dependecies_states(task)

        if all([d['state']=="done" for d in dependency_states]):
            log("dependecies complete, will unlock", task)
            self.move_task("locked", "waiting", task.filename_instance)
            return dict(state="waiting", fn=self.queue_dir("waiting") + "/" + task.filename_instance)

        if any([d['state']=="failed" for d in dependency_states]):
            log("dependecies complete, will unlock", task)
            self.move_task("locked", "failed", task.filename_instance)
            return dict(state="failed", fn=self.queue_dir("failed") + "/" + task.filename_instance)

        if not any([d['state'] in ["running","waiting","locked"] for d in dependency_states]):
            log("dependecies incomplete, but nothing will come of this anymore, will unlock", task)
            self.move_task("locked", "waiting", task.filename_instance)
            return dict(state="waiting", fn=self.queue_dir("waiting") + "/" + task.filename_instance)

        log("task still locked", task)
        return dict(state="locked",fn=self.queue_dir("locked")+"/"+task.filename_instance)
    
    def remember(self,task_data,submission_data=None):
        task=Task(task_data,submission_data=submission_data)
        nfn=self.queue_dir("problem") + "/"+task.filename_instance
        open(nfn, "w").write(task.serialize())

    def put(self,task_data,submission_data=None, depends_on=None):
        assert depends_on is None or type(depends_on) in [list,tuple]

        task=Task(task_data,submission_data=submission_data,depends_on=depends_on)

        instances_for_key=self.find_task_instances(task)
        assert len(instances_for_key)<=1

        if len(instances_for_key) == 1:
            instance_for_key=instances_for_key[0]
        else:
            instance_for_key=None

        if instance_for_key is not None and instance_for_key['state']=="locked":
            found_task=Task.from_file(instance_for_key['fn'])
            log("task found locked", found_task, "will use instead of", task)
            return self.try_to_unlock(found_task)

        if instance_for_key is not None:
            log("found existing instance(s) for this key, no need to put:",instances_for_key)
            return instance_for_key

        if depends_on is None:
            fn=self.queue_dir("waiting") + "/" + task.filename_instance
            open(fn,"w").write(task.serialize())
        else:
            fn=self.queue_dir("locked") + "/" + task.filename_instance
            open(fn, "w").write(task.serialize())

        recovered_task=Task.from_file(fn)
        if recovered_task.filename_instance != task.filename_instance:
            log("inconsitent storage:")
            log("stored:",task.filename_instance)
            log("recovered:", recovered_task.filename_instance)
    
            nfn=self.queue_dir("conflict") + "/put_original_" + task.filename_instance
            open(nfn, "w").write(task.serialize())
        
            nfn=self.queue_dir("conflict") + "/put_recovered_" + recovered_task.filename_instance
            open(nfn, "w").write(recovered_task.serialize())
            
            nfn=self.queue_dir("conflict") + "/put_stored_" + os.path.basename(fn)
            open(nfn, "w").write(open(fn).read())

            raise Exception("Inconsistent storage")

        log("successfully put in queue:",fn)
        return dict(state="submitted",fn=fn)

    def get(self):
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

        tasks=self.list("waiting")

        if len(tasks)==0:
            self.try_all_locked()
            tasks=self.list("waiting")
            if len(tasks)==0:
                raise Empty()

        task_name=tasks[-1]
                
        self.current_task = Task.from_file(self.queue_dir("waiting")+"/"+task_name)

        log(self.current_task.filename_instance,task_name)

        if self.current_task.filename_instance != task_name:
            log("inconsitent storage:")
            log(">>>> stored:", task_name)
            log(">>>> recovered:", self.current_task.filename_instance)

            fn=self.queue_dir("conflict") + "/get_stored_" + self.current_task.filename_instance
            open(fn, "w").write(self.current_task.serialize())
        
            fn=self.queue_dir("conflict") + "/get_recovered_" + task_name
            open(fn, "w").write(open(self.queue_dir("waiting")+"/"+task_name).read())

            raise Exception("Inconsistent storage")

        if not os.path.exists(self.queue_dir("waiting")+"/"+self.current_task.filename_instance):
            self.current_task = None
            raise TaskStolen()

        self.current_task_status = "waiting"
        self.clear_current_task_entry()

        log("tast is not waiting",self.task_fn)
        self.current_task_status = "running"
        self.current_task.to_file(self.task_fn)
        log("tast is running", self.task_fn)

        log('task',self.current_task.submission_info)

        return self.current_task

    def find_dependecies_states(self,task):
        if task.depends_on is None:
            raise Exception("can not inspect dependecies in an independent task!")

        dependencies=[]
        for dependency in task.depends_on:
            dependency_task=Task(dependency)
            dependency_instances=self.find_task_instances(dependency_task)

            dependencies.append(dict(states=[]))

            for i in dependency_instances:
                # if i['state']=="done"]) == 0:
                #log("dependency incomplete")
                dependencies[-1]['states'].append(i['state'])
                dependencies[-1]['task']=dependency_task

            if 'done' in dependencies[-1]['states']:
                dependencies[-1]['state']='done'
            elif 'failed' in dependencies[-1]['states']:
                dependencies[-1]['state']='failed'
            else:
                dependencies[-1]['state']='incomplete'
            
            log("dependency:",dependencies[-1]['state'],dependencies[-1]['states'], dependency, dependency_instances)

        return dependencies




    def task_locked(self,depends_on):
        log("locking task",self.current_task)
        self.clear_current_task_entry()
        self.current_task_status="locked"
        self.current_task.depends_on=depends_on
        self.current_task.to_file(self.queue_dir("locked")+"/"+self.current_task.filename_instance)

        self.current_task=None


    def task_done(self):
        log("task done",self.current_task)
        self.clear_current_task_entry()
        self.current_task_status="done"
        self.current_task.to_file(self.task_fn)

        self.current_task=None

    def task_failed(self,update=lambda x:None):
        self.clear_current_task_entry()
        self.current_task_status = "failed"

        update(self.current_task)

        self.current_task.to_file(self.task_fn)

        self.current_task = None

    def clear_current_task_entry(self,status=None):
        if status is None:
            status=self.current_task_status

        try:
            os.remove(self.queue_dir(status) + "/" + self.current_task.filename_instance)
        except OSError:
            log("current task stolen!")
            raise TaskStolen()

    def copy_task(self,fromk,tok,taskname=None):
        if taskname is None:
            taskname=self.taskname

        task=Task.from_file(self.queue_dir(fromk) + "/" + taskname)
        task.to_file(self.queue_dir(tok) + "/" + taskname)

    def move_task(self,fromk,tok,taskname=None):
        if taskname is None:
            taskname=self.taskname

        task=Task.from_file(self.queue_dir(fromk) + "/" + taskname)
        task.to_file(self.queue_dir(tok) + "/" + taskname)

        try:
            os.remove(self.queue_dir(fromk) + "/" + taskname)
        except OSError as e:
            log("locked task stolen!",taskname,"from",fromk,"to",tok,"exception:",e)

    def remove_task(self,fromk,taskname=None):
        os.remove(self.queue_dir(fromk) + "/" + taskname)

    def wipe(self,wipe_from=["waiting"],purge=True):
        for fromk in wipe_from:
            for taskname in self.list(fromk):
                if purge:
                    log("removing",self.queue_dir(fromk) + "/" + taskname)
                    os.remove(self.queue_dir(fromk) + "/" + taskname)
                else:
                    log("to delete",self.queue_dir(fromk) + "/" + taskname)
                    self.move_task(fromk,"deleted",taskname=taskname)

    def list(self,kind=None,kinds=None,fullpath=False):
        if kinds is None:
            kinds=["waiting"]
        if kind is not None:
            kinds=[kind]

        kind_jobs = []

        for kind in kinds:
            taskdir=self.queue_dir(kind)
            for fn in reversed(sorted(glob.glob(taskdir + "/*"),key=os.path.getctime)):
                if fullpath:
                    kind_jobs.append(fn)
                else:
                    kind_jobs.append(fn.replace(taskdir+"/",""))
        return kind_jobs

    @property
    def info(self):
        r={}
        for kind in "waiting","running","done","failed","locked":
            r[kind]=len(self.list(kind))
        return r

    def watch(self,delay=1):
        while True:
            log(self.info())
            time.sleep(delay)


