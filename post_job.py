import contextlib

from oslo_utils import uuidutils
from taskflow.engines import save_factory_details
from taskflow.persistence import backends as persistence_backends
from taskflow.jobs import backends as job_backends
from taskflow.persistence import models

from board import PerAppBoard
from flows import flow_factory

persistence = persistence_backends.fetch({
    "connection": "zookeeper",
    "hosts": "localhost",
    "path": "/taskflow",
})

conn = persistence.get_connection()
conn.upgrade()  # Not needed for ZK?


def get_or_create_book(name):
    for lb in conn.get_logbooks():
        if lb.name == name:
            return lb

    return models.LogBook(name)


book = get_or_create_book("henkslaaf")

flow_detail = models.FlowDetail("some flow (testflow)", uuid=uuidutils.generate_uuid())
book.add(flow_detail)

conn.save_logbook(book)

save_factory_details(flow_detail,
                     flow_factory, (), {},
                     backend=persistence)


board = PerAppBoard('my-board', {
    "hosts": "localhost",
}, persistence=persistence)

# board = job_backends.fetch("my-board", {
#     "board": "zookeeper",
#     "hosts": "localhost",
# }, persistence=persistence)
board.connect()

with contextlib.closing(board):
    job = board.post("my-first-job", book, details={'flow_uuid': flow_detail.uuid,
                                                    'store': {'msg': 'hoi'},
                                                    'app': 'henkslaaf'})
