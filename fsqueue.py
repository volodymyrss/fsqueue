from __future__ import print_function, division

import yaml
import os
import time
import socket
from hashlib import sha224

import glob

class Empty(Exception):
    pass

class CurrentTaskUnfinished(Exception):
    pass

class Task(object):
    def __init__(self,task_data, shortname=None,completename=None,execution_info=None, submission_data=None, depends_on=None):
        self.task_data=task_data
        self.shortname = shortname
        self.completename=completename
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
    def from_file(cls,fn,completename=None):
        task_dict=yaml.load(open(fn))

        self=cls(task_dict['task_data'],completename=completename)
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
        if self.completename is not None:
            return self.completename

        filename_components=[]

        #print("encoding:")
        #print(str(self.task_data).encode('utf-8'))

        filename_components.append(sha224(str(self.task_data).encode('utf-8')).hexdigest()[:8])

        if self.shortname is not None:
            filename_components.append(self.shortname)

        if not key:
            filename_components.append("%.14lg"%self.submission_info['time'])
            filename_components.append(self.submission_info['utc'])

            filename_components.append(sha224(str(self.submission_info).encode('utf-8')).hexdigest()[:8])

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


    def put(self,task_data,shortname=None,submission_data=None, depends_on=None):
        assert depends_on is None or type(depends_on) in [list,tuple]

        task=Task(task_data,shortname,submission_data=submission_data,depends_on=depends_on)

        instances_for_key=self.find_task_instances(task)

        locked_instances=[i for i in instances_for_key if i['state'] == "locked"]
        if len(locked_instances)>0:
            assert len(locked_instances)==1

            found_task=Task.from_file(locked_instances[0]['fn'])
            print("task found locked",found_task,"will use instead of",task)
            if len(self.find_incomplete_dependecies(found_task))==0:
                print("dependecies complete, will unlock",found_task)
                self.move_task("locked","waiting",found_task.filename_instance)
                return
            else:
                print("task still locked", task)
                return locked_instances


        if len(instances_for_key)>0:
            print("found existing instance(s) for this key, no need to put:",instances_for_key)
            return instances_for_key

        if depends_on is None:
            fn=self.queue_dir("waiting") + "/" + task.filename_instance
            open(fn,"w").write(task.serialize())
        else:
            fn=self.queue_dir("locked") + "/" + task.filename_instance
            open(fn, "w").write(task.serialize())

        return dict(state="submitted",fn=fn)

    def get(self):
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

        tasks=self.list()

        if len(tasks)==0:
            raise Empty()

        task_name=tasks[-1]

        self.current_task = Task.from_file(self.queue_dir("waiting")+"/"+task_name, completename=task_name)
        self.current_task_status = "waiting"
        self.clear_current_task_entry()

        self.current_task_status = "running"
        self.current_task.to_file(self.task_fn)

        print('task',self.current_task.submission_info)

        return self.current_task

    def find_incomplete_dependecies(self,task):
        assert task.depends_on is not None, "can not inspect dependecies in an independent task!"

        incomplete_dependencies=[]
        for dependency in task.depends_on:
            dependency_task=Task(dependency)
            dependency_instances=self.find_task_instances(dependency_task)
            print("dependency:", dependency, dependency_instances)
            if len([i for i in dependency_instances if i['state']=="done"]) == 0:
                print("dependency incomplete")
                incomplete_dependencies.append(dependency_task)

        return incomplete_dependencies




    def task_locked(self):
        self.clear_current_task_entry()
        self.current_task_status="locked"
        self.current_task=None


    def task_done(self):
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
        os.remove(self.queue_dir(status) + "/" + self.current_task.filename_instance)

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
        os.remove(self.queue_dir(fromk) + "/" + taskname)

    def remove_task(self,fromk,taskname=None):
        os.remove(self.queue_dir(fromk) + "/" + taskname)

    def wipe(self,wipe_from=["waiting"]):
        for fromk in wipe_from:
            for taskname in self.list(fromk):
                self.move_task(fromk,"deleted",taskname=taskname)

    def list(self,kind="waiting",fullpath=False):
        taskdir=self.queue_dir(kind)
        kind_jobs=[]
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
            print(self.info())
            time.sleep(delay)


