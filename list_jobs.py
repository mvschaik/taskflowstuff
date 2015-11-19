import contextlib

from taskflow.persistence import backends as persistence_backends
from taskflow.jobs import backends as job_backends
import logging

from board import HypernodeJobBoard

logging.basicConfig(level=logging.WARNING)

persistence = persistence_backends.fetch({
    "connection": "zookeeper",
    "hosts": "localhost",
    "path": "/taskflow",
})

# board = job_backends.fetch("my-board", {
#     "board": "zookeeper",
#     "hosts": "localhost",
# }, persistence=persistence)

board = HypernodeJobBoard('my-board', {
    "hosts": "localhost",
}, persistence=persistence)
board.connect()


with contextlib.closing(board):
    print("All jobs:")
    for job in board.iterjobs(ensure_fresh=True, only_unclaimed=False):
        print job

    print("Unclaimed:")
    for job in board.iterjobs(ensure_fresh=True, only_unclaimed=True):
        print job
