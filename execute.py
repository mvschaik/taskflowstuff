import contextlib
import logging


from taskflow.conductors import backends as conductors
from taskflow.persistence import backends as persistence_backends

from board import PerAppBoard

logging.basicConfig(level=logging.WARNING)

persistence = persistence_backends.fetch({
    "connection": "zookeeper",
    "hosts": "localhost",
    "path": "/taskflow",
})

board = PerAppBoard('my-board', {
    "hosts": "localhost",
}, persistence=persistence)


# board = job_backends.fetch("my-board", {
#     "board": "zookeeper",
#     "hosts": "localhost",
#     "path": "/jobboard",
# }, persistence=persistence)
board.connect()

conductor = conductors.fetch("blocking", "executor 1", board, engine="parallel", wait_timeout=.1)

with contextlib.closing(board):
    conductor.run()
