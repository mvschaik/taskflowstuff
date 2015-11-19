import logging

import six
from taskflow import states
from taskflow.jobs.base import JobBoardIterator
from taskflow.jobs.backends.impl_zookeeper import ZookeeperJobBoard

LOG = logging.getLogger(__name__)


class MultiQueueIterator(six.Iterator):
    """ Job board iterator that iterates over queues

    To have parallel job queues inside a job board in which the jobs will
    be sequentially processed per queue, this implementation uses a queue
    function to determine which (virtual) queue a job belongs.
    When iterating, it only returns the first element of the queue to ensure
    no more than 1 job per virtual queue will be taken.

    The constructor takes an extra argument ``queue_fn`` that takes a job and
    returns a queue index.
    """
    _UNCLAIMED_JOB_STATES = (states.UNCLAIMED,)

    def __init__(self, board, logger, queue_fn,
                 board_fetch_func=None, board_removal_func=None,
                 only_unclaimed=False, ensure_fresh=False):

        # Internal iterator returns all jobs, not only unclaimed, since we need
        # the first item in each queue, even if it's claimed.
        self._internal_iterator = JobBoardIterator(board, logger,
                                                   board_fetch_func=board_fetch_func,
                                                   board_removal_func=board_removal_func,
                                                   only_unclaimed=False,
                                                   ensure_fresh=ensure_fresh)
        self._queue_fn = queue_fn
        self._seen_queues = set()
        self._only_unclaimed = only_unclaimed

    def __iter__(self):
        return self

    def __next__(self):
        if not self._only_unclaimed:
            return next(self._internal_iterator)

        while True:
            candidate = next(self._internal_iterator)
            if candidate is None:
                # Empty!
                return None

            queue = self._queue_fn(candidate)
            if queue not in self._seen_queues:
                self._seen_queues.add(queue)
                if candidate.state in self._UNCLAIMED_JOB_STATES:
                    return candidate


class HypernodeJobBoard(ZookeeperJobBoard):
    """ Job board that uses a queue per app """
    def iterjobs(self, only_unclaimed=True, ensure_fresh=False):
        return MultiQueueIterator(
            self, LOG, only_unclaimed=only_unclaimed,
            ensure_fresh=ensure_fresh, board_fetch_func=self._fetch_jobs,
            board_removal_func=lambda a_job: self._remove_job(a_job.path),
            queue_fn=lambda a_job: a_job.details['app']
        )
