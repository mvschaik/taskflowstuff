import contextlib

from taskflow.persistence import backends as persistence_backends
from taskflow.jobs import backends as job_backends
import logging

logging.basicConfig(level=logging.WARNING)

persistence = persistence_backends.fetch({
    "connection": "zookeeper",
    "hosts": "localhost",
    "path": "/taskflow",
})

board = job_backends.fetch("my-board", {
    "board": "zookeeper",
    "hosts": "localhost",
    "path": "/jobboard",
}, persistence=persistence)
board.connect()


with contextlib.closing(board):
    for job in board.iterjobs(ensure_fresh=True):
        print job
