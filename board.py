import logging
import threading

from kazoo.recipe import watchers
from taskflow.jobs import base
from taskflow.utils import misc
from oslo_serialization import jsonutils
from oslo_utils import uuidutils
from taskflow.jobs.backends.impl_zookeeper import ZookeeperJobBoard, ZookeeperJob
from kazoo.protocol import paths as k_paths

LOG = logging.getLogger(__name__)


class PerAppBoard(ZookeeperJobBoard):
    def __init__(self, *args, **kwargs):
        super(PerAppBoard, self).__init__(*args, **kwargs)
        self._app_cond = threading.Condition()
        self._apps = []

    def post(self, name, book=None, details=None):
        # NOTE(harlowja): Jobs are not ephemeral, they will persist until they
        # are consumed (this may change later, but seems safer to do this until
        # further notice).
        job_uuid = uuidutils.generate_uuid()
        job_posting = base.format_posting(job_uuid, name,
                                          book=book, details=details)
        raw_job_posting = misc.binary_encode(jsonutils.dumps(job_posting))
        with self._wrap(job_uuid, None,
                        fail_msg_tpl="Posting failure: %s",
                        ensure_known=False):
            job_path = self._client.create(k_paths.join(self.path, details['app'], self.JOB_PREFIX),
                                           value=raw_job_posting,
                                           sequence=True,
                                           ephemeral=False,
                                           makepath=True)
            job = ZookeeperJob(self, name, self._client, job_path,
                               backend=self._persistence,
                               book=book, details=details, uuid=job_uuid,
                               book_data=job_posting.get('book'))
            with self._job_cond:
                self._known_jobs[job_path] = job
                self._job_cond.notify_all()
            self._emit(base.POSTED, details={'job': job})
            return job

    def _on_real_job_posting(self, *args, **kwargs):
        super(PerAppBoard, self)._on_job_posting(*args, **kwargs)

    def _on_job_posting(self, children, delayed=True):
        for c in children:
            app_path = k_paths.join(self.path, c)

            watchers.ChildrenWatch(
                    self._client,
                    app_path,
                    func=self._on_real_job_posting,
                    allow_session_lost=True)
