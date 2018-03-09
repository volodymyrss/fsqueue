from __future__ import print_function

import pytest
import glob
import time

def test_one():
    import fsqueue

    queue=fsqueue.Queue("./queue")
    queue.wipe(["waiting","done","running"])

    assert queue.info['waiting']==0

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)

    assert queue.put(t1)['state'] == "submitted"

    assert queue.info['waiting'] == 1

    assert queue.put(t1)['state'] == "waiting"

    time.sleep(0.1)

    assert queue.put(t2, shortname="custom_job")['state'] == "submitted"

    assert queue.info['waiting'] == 2

    print(queue.info)

    print(glob.glob(queue.queue_dir("waiting")+"/*"))

    assert len(queue.list()) == 2
    print(queue.info)

    assert len(queue.list()) == 2
    print(queue.info)

    t=queue.get().task_data

    assert queue.info['waiting'] == 1
    assert queue.info['running'] == 1

    print("from queue",t)
    print("original",t1)

    assert t==t1
    print(queue.info)

    with pytest.raises(fsqueue.CurrentTaskUnfinished):
        t=queue.get()

    print(queue.info)

    queue.task_done()
    print(queue.info)

    t = queue.get().task_data
    assert t==t2
    queue.task_failed()
    print(queue.info)

    with pytest.raises(fsqueue.Empty):
        queue.get()


    print(queue.info)


def test_locked_jobs():
    import fsqueue

    queue=fsqueue.Queue("./queue")
    queue.wipe(["waiting","done","running","locked","failed"])

    assert queue.info['waiting']==0

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)

    assert queue.put(t1,depends_on=[t2])['state']=="submitted"

    time.sleep(0.1)
    queue.put(t2, shortname="custom_job")

    print(queue.info)

    print(glob.glob(queue.queue_dir("waiting")+"/*"))

    assert len(queue.list("waiting")) == 1
    assert len(queue.list("locked")) == 1
    print(queue.info)

    print("trying to put dependent again")
    assert queue.put(t1) is not None


    t=queue.get().task_data

    print("from queue",t)
    print("original",t2)

    queue.task_done()
    print("finished dependency")

    print(queue.info)

    print("expected resolved dependecy`")
    assert queue.put(t1) is None

    assert len(queue.list("waiting")) == 1
    assert len(queue.list("locked")) == 0

    t = queue.get().task_data

    print("from queue", t)
    print("original", t1)

    assert t == t1

    queue.task_done()
    with pytest.raises(fsqueue.Empty):
        queue.get()
    print(queue.info)
