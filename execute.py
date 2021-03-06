import contextlib
import copy_reg
import functools
import logging
import threading
import types

import coloredlogs
import six
from futurist import ProcessPoolExecutor, ThreadPoolExecutor
from oslo_utils import excutils
from taskflow.conductors import backends as conductors
from taskflow.jobs.backends.impl_zookeeper import ZookeeperJob
from taskflow.persistence import backends as persistence_backends
from taskflow.listeners import logging as logging_listener
from taskflow.utils import async_utils
from taskflow.conductors import base
from taskflow.types import timing as tt
try:
    from contextlib import ExitStack  # noqa
except ImportError:
    from contextlib2 import ExitStack  # noqa
from taskflow import exceptions as excp
from board import HypernodeJobBoard
from taskflow import engines

LOG = logging.getLogger(__name__)

# coloredlogs.install(level='DEBUG')
WAIT_TIMEOUT = 0.5
NO_CONSUME_EXCEPTIONS = tuple([
    excp.ExecutionFailure,
    excp.StorageFailure,
])

logging.basicConfig(level=logging.WARN)

pool = ProcessPoolExecutor(100)


def _run_job(flow_detail, store, engine, persistence, engine_options):
    engine = engines.load_from_detail(flow_detail, store=store,
                                      engine=engine,
                                      backend=persistence,
                                      **engine_options)
    with logging_listener.LoggingListener(engine, log=LOG):
        LOG.debug("Dispatching engine for job")
        consume = True
        try:
            for stage_func, event_name in [(engine.compile, 'compilation'),
                                           (engine.prepare, 'preparation'),
                                           (engine.validate, 'validation'),
                                           (engine.run, 'running')]:
                # self._notifier.notify("%s_start" % event_name, {
                #     'job': job,
                #     'engine': engine,
                #     'conductor': self,
                # })
                stage_func()
                # self._notifier.notify("%s_end" % event_name, {
                #     'job': job,
                #     'engine': engine,
                #     'conductor': self,
                # })
        except excp.WrappedFailure as e:
            if all((f.check(*NO_CONSUME_EXCEPTIONS) for f in e)):
                consume = False
            if LOG.isEnabledFor(logging.WARNING):
                # if consume:
                #     LOG.warn("Job execution failed (consumption being"
                #              " skipped): %s [%s failures]", job, len(e))
                # else:
                #     LOG.warn("Job execution failed (consumption"
                #              " proceeding): %s [%s failures]", job, len(e))
                # Show the failure/s + traceback (if possible)...
                for i, f in enumerate(e):
                    LOG.warn("%s. %s", i + 1, f.pformat(traceback=True))
        except NO_CONSUME_EXCEPTIONS:
            # LOG.warn("Job execution failed (consumption being"
            #          " skipped): %s", job, exc_info=True)
            consume = False
        except Exception:
            # LOG.warn("Job execution failed (consumption proceeding): %s",
            #          job, exc_info=True)
            pass
        else:
            pass
            # LOG.info("Job completed successfully: %s", job)
        return consume

class AsyncConductor(base.Conductor):
    """A conductor that dispatches jobs to separate processes

    This conductor iterates over jobs in the provided jobboard, start a process
    to work on the job, and then consume those work units after completion. This
    process will repeat until the conductor has been stopped or other critical
    error occurs.
    """

    START_FINISH_EVENTS_EMITTED = tuple([
        'compilation', 'preparation',
        'validation', 'running',
    ])
    """Events will be emitted for the start and finish of each engine
       activity defined above, the actual event name that can be registered
       to subscribe to will be ``${event}_start`` and ``${event}_end`` where
       the ``${event}`` in this pseudo-variable will be one of these events.
    """
    def __init__(self, name, jobboard,
                 persistence=None, engine=None,
                 engine_options=None, wait_timeout=None):
        super(AsyncConductor, self).__init__(
            name, jobboard, persistence=persistence,
            engine=engine, engine_options=engine_options)
        if wait_timeout is None:
            wait_timeout = WAIT_TIMEOUT
        if isinstance(wait_timeout, (int, float) + six.string_types):
            self._wait_timeout = tt.Timeout(float(wait_timeout))
        elif isinstance(wait_timeout, tt.Timeout):
            self._wait_timeout = wait_timeout
        else:
            raise ValueError("Invalid timeout literal: %s" % (wait_timeout))
        self._dead = threading.Event()

    def stop(self):
        """Requests the conductor to stop dispatching.

        This method can be used to request that a conductor stop its
        consumption & dispatching loop.

        The method returns immediately regardless of whether the conductor has
        been stopped.
        """
        self._wait_timeout.interrupt()

    @property
    def dispatching(self):
        return not self._dead.is_set()

    def _listeners_from_job(self, job, engine):
        listeners = super(AsyncConductor, self)._listeners_from_job(job, engine)
        listeners.append(logging_listener.LoggingListener(engine, log=LOG))
        return listeners

    def _dispatch_job(self, job):
        flow_detail = self._flow_detail_from_job(job)
        if job.details and 'store' in job.details:
            store = dict(job.details["store"])
        else:
            store = {}
        return pool.submit(_run_job, flow_detail, store, self._engine, self._persistence, self._engine_options)

    def _job_done(self, job, f):
        consume = f.result()
        try:
            if consume:
                self._jobboard.consume(job, self._name)
            else:
                self._jobboard.abandon(job, self._name)
        except (excp.JobFailure, excp.NotFound):
            if consume:
                LOG.warn("Failed job consumption: %s", job,
                         exc_info=True)
            else:
                LOG.warn("Failed job abandonment: %s", job,
                         exc_info=True)

    def run(self):
        self._dead.clear()

        try:
            while True:
                if self._wait_timeout.is_stopped():
                    break
                for job in self._jobboard.iterjobs():
                    if self._wait_timeout.is_stopped():
                        break
                    LOG.debug("Trying to claim job: %s", job)
                    try:
                        self._jobboard.claim(job, self._name)
                    except (excp.UnclaimableJob, excp.NotFound):
                        LOG.debug("Job already claimed or consumed: %s", job)
                        continue
                    try:
                        f = self._dispatch_job(job)
                        f.add_done_callback(functools.partial(self._job_done, job))
                    except KeyboardInterrupt:
                        with excutils.save_and_reraise_exception():
                            LOG.warn("Job dispatching interrupted: %s", job)
                    except Exception:
                        LOG.warn("Job dispatching failed: %s", job,
                                 exc_info=True)

                if not self._wait_timeout.is_stopped():
                    self._wait_timeout.wait()

        except StopIteration:
            pass
        finally:
            self._dead.set()

    def wait(self, timeout=None):
        """Waits for the conductor to gracefully exit.

        This method waits for the conductor to gracefully exit. An optional
        timeout can be provided, which will cause the method to return
        within the specified timeout. If the timeout is reached, the returned
        value will be False.

        :param timeout: Maximum number of seconds that the :meth:`wait` method
                        should block for.
        """
        return self._dead.wait(timeout)


def main():
    persistence = persistence_backends.fetch({
        'connection': 'sqlite:////tmp/taskflow.db'

    })

    board = HypernodeJobBoard('my-board', {
        "hosts": "localhost",
    }, persistence=persistence)

    # board = job_backends.fetch("my-board", {
    #     "board": "zookeeper",
    #     "hosts": "localhost",
    #     "path": "/jobboard",
    # }, persistence=persistence)
    board.connect()

    # conductor = conductors.fetch("blocking", "executor 1", board, engine="parallel", wait_timeout=.1)
    conductor = AsyncConductor("async", board, engine="parallel")

    with contextlib.closing(board):
        conductor.run()


if __name__ == '__main__':
    main()
