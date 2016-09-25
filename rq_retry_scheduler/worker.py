from datetime import timedelta
from functools import partial
import logging
import rq

from .queue import Queue

logger = logging.getLogger('rq.worker')


class Worker(rq.Worker):

    retry_delays = {
        1: timedelta(minutes=1),
        2: timedelta(minutes=5),
        3: timedelta(minutes=10),
    }

    def __init__(self, queues, queue_class=None, connection=None,
                 *args, **kwargs):

        if queue_class is None or not issubclass(queue_class, Queue):
            queue_class = Queue
            q = partial(Queue, connection=connection)

            queues = [
                q(queue.name) if isinstance(queue, rq.Queue) else queue
                for queue in rq.utils.ensure_list(queues)]

        super(Worker, self).__init__(queues, *args, queue_class=queue_class,
                                     connection=connection, **kwargs)

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
