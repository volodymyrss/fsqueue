import pytest

def test_one():
    import fsqueue

    queue=fsqueue.Queue("./queue")
    queue.wipe()

    t1=dict(test=1,data=2)
    t2 = dict(test=1, data=3)

    queue.put(t1,shortname="2")
    queue.put(t2)
    print(queue.info)

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
