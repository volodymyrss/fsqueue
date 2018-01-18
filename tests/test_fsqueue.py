import pytest
import glob

def test_one():
    import fsqueue

    queue=fsqueue.Queue("./queue")
    queue.wipe()

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)

    assert queue.put(t1) is None
    assert queue.put(t1) is not None
    queue.put(t2, shortname="custom_job")

    print(queue.info)

    print glob.glob(queue.queue_dir("waiting")+"/*")

    assert len(queue.list())==2
    print(queue.info)

    assert len(queue.list()) == 2
    print(queue.info)

    t=queue.get()
    assert t==t1
    print(queue.info)

    with pytest.raises(fsqueue.CurrentTaskUnfinished):
        t=queue.get()

    print(queue.info)

    queue.task_done()
    print(queue.info)

    t = queue.get()
    assert t==t2
    queue.task_failed()
    print(queue.info)

    with pytest.raises(fsqueue.Empty):
        queue.get()


    print(queue.info)
