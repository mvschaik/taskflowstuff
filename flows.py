from time import sleep

from taskflow import task
from taskflow.patterns import linear_flow as lf


class TestTask(task.Task):
    version = 2

    def execute(self, timeout=5, app=None):
        print("starting %s for app %s" % (self, app))
        time = timeout
        while time > 0:
            sleep(1)
            self.update_progress(1.0 - time/timeout)
            time -= 1

        print("Done %s" % self)
        return 'ok'


class PrintTask(task.Task):
    def execute(self, msg):
        print("Printing message: %s" % msg)


def flow_factory():
    return lf.Flow('testflow').add(
        TestTask(name='first'),
        PrintTask(name='print'),
        TestTask(name='last'))
