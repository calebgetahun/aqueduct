import os
from client import AqueductClient

dsn = os.environ["AQUEDUCT_DATABASE_URL"]
client = AqueductClient(dsn)

# TODO: demonstrate enqueue, cancel_job, inspect_job
