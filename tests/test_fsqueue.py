import pytest

def test_one():
    import fsqueue

    queue=fsqueue.Queue("./queue")
    queue.wipe()

    t1=dict(test=1,data=2)
    t2 = dict(test=1, data=3)

    queue.put(t1,shortname="2")
    queue.put(t2)

    assert len(queue.list())==2
    assert len(queue.list()) == 2

    t=queue.get()
    assert t==t1

    t=queue.get()
    assert t==t2

    with pytest.raises(fsqueue.Empty):
        queue.get()
