from time import sleep

from taskflow import task
from taskflow.patterns import linear_flow as lf


class TestTask(task.Task):
    def execute(self):
        print("Executing %s" % self)
        return 'ok'


class PrintTask(task.Task):
    def execute(self, msg):
        raise RuntimeError("Oeps!")
        print("Printing message: %s" % msg)


def flow_factory():
    return lf.Flow('testflow').add(
        TestTask(name='first'),
        PrintTask(name='print'),
        TestTask(name='last'))
