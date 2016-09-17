from datetime import timedelta
import logging
from rq import Worker

from .queue import Queue

logger = logging.getLogger('rq:retryscheduler:worker')
logger.setLevel('DEBUG')
logger.addHandler(logging.StreamHandler())

class Worker(Worker):

    retry_delays = {
        1: timedelta(seconds=1),
        2: timedelta(seconds=5),
        3: timedelta(seconds=10),
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.log = logger
        self.push_exc_handler(self.exc_handler)

    def exc_handler(self, job, exc_type, exc_value, traceback):
        job.meta.setdefault('failures', 0)
        job.meta['failures'] += 1

        self.log.info("Job {}: Exception handler with {} failures".format(
                        job.id, job.meta['failures']))

        delay = self.retry_delays.get(job.meta['failures'])

        if delay is None:
            self.log.warn(
                "Job {}: Failed too many times, not requeuing".format(job.id))
            job.save()
            return True

        self.log.info("Job {}: Requeueing in {} into queue {}".format(
                       job.id, delay, job.origin))

        queue = self.queue_class(job.origin, connection=self.connection)
        queue.enqueue_job_in(delay, job)
        return False
