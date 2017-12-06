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
    def __init__(self,task_data, shortname=None,completename=None):
        self.task_data=task_data
        self.shortname = shortname
        self.completename=completename
        self.submission_info=self.construct_submission_info()

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
            ),
            default_flow_style=False, default_style=''
        )


    @classmethod
    def from_file(cls,fn,completename=None):
        task_dict=yaml.load(open(fn))

        self=cls(task_dict['task_data'],completename=completename)
        self.submission_info=task_dict['submission_info']

        return self

    @property
    def filename(self):
        if self.completename is not None:
            return self.completename

        filename="%.14lg"%self.submission_info['time']
        filename+="_"+self.submission_info['utc']

        filename+="_"+sha224(str(self.submission_info).encode('utf-8')).hexdigest()[:8]
        filename +="_"+ sha224(str(self.task_data).encode('utf-8')).hexdigest()[:8]

        if self.shortname is not None:
            filename+="_"+self.shortname

        return filename

def makedir_if_neccessary(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != 17: raise


class Queue(object):
    def __init__(self,root_directory):
        self.root_directory=root_directory
        self.init_directory_tree()
        self.current_task=None

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

    def put(self,task_data,shortname=None):
        task=Task(task_data,shortname)
        open(self.queue_dir("waiting")+"/"+task.filename,"w").write(task.serialize())

    def get(self):
        if self.current_task is not None:
            raise CurrentTaskUnfinished(self.current_task)

        tasks=self.list()

        if len(tasks)==0:
            raise Empty()

        task_name=tasks[-1]
        self.current_task = Task.from_file(self.queue_dir("waiting")+"/"+task_name, completename=task_name)
        self.move_task(task_name,"waiting","running")

        print('task',self.current_task.submission_info)

        return self.current_task.task_data

    def task_done(self):
        self.move_task(self.current_task.filename,"running","done")
        self.current_task=None

    def task_failed(self):
        self.move_task(self.current_task.filename, "running", "failed")
        self.current_task = None

    def move_task(self,taskname,fromk,tok):
        os.rename(
            self.queue_dir(fromk) + "/" + taskname,
            self.queue_dir(tok) + "/" + taskname,
        )

    def wipe(self):
        for taskname in self.list('waiting'):
            self.move_task(taskname,"waiting","deleted")

    def list(self,kind="waiting",fullpath=False):
        taskdir=self.queue_dir(kind)
        waiting_jobs=[]
        for fn in reversed(sorted(glob.glob(taskdir + "/*"))):
            if fullpath:
                waiting_jobs.append(fn)
            else:
                waiting_jobs.append(fn.replace(taskdir+"/",""))
        return waiting_jobs

